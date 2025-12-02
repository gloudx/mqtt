package evstore

import (
	"bytes"
	"errors"
	"mqtt-http-tunnel/improved/hlc"
	"mqtt-http-tunnel/improved/vclock"
	"sort"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

// Prefixes для ключей в BadgerDB
var (
	prefixRecords  = []byte("r/") // r/{TID} → Record
	prefixDAG      = []byte("d/") // d/{CID} → []CID (parents)
	prefixCIDIndex = []byte("i/") // i/{CID} → TID
	prefixHeads    = []byte("h/") // h/{CID} → empty (текущие heads)
	keyHLC         = []byte("meta/hlc")
	keyProcessID   = []byte("meta/process_id")
)

// EventHandler обработчик новых записей
type EventHandler func(CID, *Record)

// StoreConfig конфигурация хранилища
type StoreConfig struct {
	Path          string
	ProcessID     string        // ID этого процесса (для VC)
	MergeStrategy MergeStrategy // Стратегия разрешения форков
	Logger        zerolog.Logger
}

// Store - распределённое хранилище событий на основе Merkle-DAG
// Реализует концепции из теории:
// - Система переходов (каждая запись = событие/переход)
// - Причинно-следственный порядок через DAG структуру
// - HLC + Vector Clocks для временных меток
// - Детерминистическое разрешение форков
type Store struct {
	db          *badger.DB
	clock       *hlc.Clock
	vc          *vclock.VectorClock
	processID   string
	heads       CIDSet
	resolver    *MergeResolver
	subscribers []EventHandler
	//
	mu     sync.RWMutex
	logger zerolog.Logger
}

// OpenStore открывает или создаёт хранилище
func OpenStore(cfg *StoreConfig) (*Store, error) {
	opts := badger.DefaultOptions(cfg.Path).
		WithLoggingLevel(badger.ERROR).
		WithSyncWrites(true) // Надёжность важнее скорости

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	processID := cfg.ProcessID
	if processID == "" {
		processID = generateProcessID()
	}

	s := &Store{
		db:        db,
		clock:     hlc.New(processID),
		vc:        vclock.New(),
		processID: processID,
		heads:     NewCIDSet(),
		resolver:  NewMergeResolver(cfg.MergeStrategy),
		logger:    cfg.Logger.With().Str("component", "evstore").Logger(),
	}

	if err := s.loadState(); err != nil {
		db.Close()
		return nil, err
	}

	s.logger.Info().
		Str("process_id", processID).
		Int("heads", s.heads.Len()).
		Msg("store opened")

	return s, nil
}

// generateProcessID генерирует уникальный ID
func generateProcessID() string {
	return hlc.New("").TID().String()[:8]
}

// Close закрывает хранилище
func (s *Store) Close() error {
	return s.db.Close()
}

// loadState загружает состояние из БД
func (s *Store) loadState() error {
	return s.db.View(func(txn *badger.Txn) error {
		// Загружаем HLC
		if item, err := txn.Get(keyHLC); err == nil {
			item.Value(func(val []byte) error {
				if ts, err := hlc.UnmarshalTimestamp(val); err == nil {
					s.clock.Set(ts)
				}
				return nil
			})
		}

		// Загружаем ProcessID (если сохранён)
		if item, err := txn.Get(keyProcessID); err == nil {
			item.Value(func(val []byte) error {
				s.processID = string(val)
				return nil
			})
		}

		// Загружаем Heads
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixHeads
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if len(key) == len(prefixHeads)+32 {
				var cid CID
				copy(cid[:], key[len(prefixHeads):])
				s.heads.Add(cid)
			}
		}

		return nil
	})
}

// Subscribe подписывает на новые записи
func (s *Store) Subscribe(handler EventHandler) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = append(s.subscribers, handler)
	index := len(s.subscribers) - 1
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if index < len(s.subscribers) {
			s.subscribers[index] = nil
		}
	}
}

func (s *Store) notify(cid CID, record *Record) {
	s.mu.RLock()
	handlers := make([]EventHandler, 0, len(s.subscribers))
	for _, h := range s.subscribers {
		if h != nil {
			handlers = append(handlers, h)
		}
	}
	s.mu.RUnlock()

	for _, h := range handlers {
		func() {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error().Interface("panic", r).Msg("subscriber panic")
				}
			}()
			h(cid, record)
		}()
	}
}

// ProcessID возвращает ID процесса
func (s *Store) ProcessID() string {
	return s.processID
}

// Clock возвращает HLC
func (s *Store) Clock() *hlc.Clock {
	return s.clock
}

// VectorClock возвращает текущие векторные часы
func (s *Store) VectorClock() *vclock.VectorClock {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.vc.Clone()
}

// Heads возвращает текущие heads (листья DAG)
func (s *Store) Heads() []CID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sortedHeads()
}

func (s *Store) sortedHeads() []CID {
	result := s.heads.ToSlice()
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i][:], result[j][:]) < 0
	})
	return result
}

// HasFork проверяет наличие форка (>1 head)
func (s *Store) HasFork() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.heads.Len() > 1
}

// Append добавляет новую запись
func (s *Store) Append(data []byte) (*Record, error) {
	s.mu.Lock()

	// Генерируем временные метки
	tid := s.clock.TID()
	s.vc.Tick(s.processID)
	vc := s.vc.Clone()

	// Текущие heads становятся родителями
	parents := s.sortedHeads()

	record := NewRecord(s.processID, tid, vc, parents, data)
	cid := record.CID()

	err := s.db.Update(func(txn *badger.Txn) error {
		return s.putRecord(txn, cid, record, true)
	})

	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	s.logger.Debug().
		Str("cid", cid.Short()).
		Str("tid", tid.String()).
		Int("parents", len(parents)).
		Msg("appended")

	s.notify(cid, record)
	return record, nil
}

// Put сохраняет внешнюю запись (при синхронизации)
func (s *Store) Put(record *Record) (CID, error) {
	cid := record.CID()

	s.mu.Lock()

	if s.has(cid) {
		s.mu.Unlock()
		return cid, nil // Уже есть
	}

	// Обновляем HLC
	s.clock.UpdateTID(record.TID)

	// Обновляем Vector Clock
	if record.VC != nil {
		s.vc.Merge(record.VC)
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		// НЕ обновляем heads при Put - это делается через Merge
		return s.putRecord(txn, cid, record, false)
	})

	s.mu.Unlock()

	if err != nil {
		return CID{}, err
	}

	s.logger.Debug().
		Str("cid", cid.Short()).
		Str("tid", record.TID.String()).
		Msg("put external record")

	return cid, nil
}

// putRecord сохраняет запись в БД
func (s *Store) putRecord(txn *badger.Txn, cid CID, record *Record, updateHeads bool) error {
	// Сохраняем запись по TID
	recordKey := append(prefixRecords, record.TID.Bytes()...)
	if err := txn.Set(recordKey, record.Marshal()); err != nil {
		return err
	}

	// DAG: CID → Parents
	dagVal := make([]byte, len(record.Parents)*32)
	for i, p := range record.Parents {
		copy(dagVal[i*32:], p[:])
	}
	dagKey := append(prefixDAG, cid[:]...)
	if err := txn.Set(dagKey, dagVal); err != nil {
		return err
	}

	// Index: CID → TID
	indexKey := append(prefixCIDIndex, cid[:]...)
	if err := txn.Set(indexKey, record.TID.Bytes()); err != nil {
		return err
	}

	// Обновляем heads
	if updateHeads {
		// Добавляем новый head
		headKey := append(prefixHeads, cid[:]...)
		if err := txn.Set(headKey, nil); err != nil {
			return err
		}
		s.heads.Add(cid)

		// Удаляем родителей из heads
		for _, p := range record.Parents {
			if s.heads.Has(p) {
				oldHeadKey := append(prefixHeads, p[:]...)
				txn.Delete(oldHeadKey)
				s.heads.Remove(p)
			}
		}
	}

	// Сохраняем HLC
	return txn.Set(keyHLC, s.clock.Timestamp().Marshal())
}

// Merge интегрирует внешние CID в heads
// Возвращает отсутствующие CID, которые нужно запросить
func (s *Store) Merge(cids []CID) (missing []CID, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := false
	missingSet := NewCIDSet()

	for _, cid := range cids {
		if !s.has(cid) {
			missingSet.Add(cid)
			continue
		}

		// Получаем родителей
		parents, err := s.parents(cid)
		if err != nil {
			s.logger.Warn().Err(err).Str("cid", cid.Short()).Msg("failed to get parents")
			continue
		}

		// Проверяем родителей
		for _, p := range parents {
			if !s.has(p) {
				missingSet.Add(p)
			}
		}

		// Добавляем в heads
		if !s.heads.Has(cid) {
			s.heads.Add(cid)
			changed = true
		}

		// Удаляем родителей из heads
		for _, p := range parents {
			if s.heads.Has(p) {
				s.heads.Remove(p)
				changed = true
			}
		}
	}

	if changed {
		err = s.db.Update(func(txn *badger.Txn) error {
			// Перезаписываем все heads
			// Сначала удаляем старые
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefixHeads
			it := txn.NewIterator(opts)
			for it.Rewind(); it.Valid(); it.Next() {
				txn.Delete(it.Item().KeyCopy(nil))
			}
			it.Close()

			// Записываем новые
			for head := range s.heads {
				key := append(prefixHeads, head[:]...)
				if err := txn.Set(key, nil); err != nil {
					return err
				}
			}
			return nil
		})

		if err == nil {
			s.logger.Debug().Int("heads", s.heads.Len()).Msg("heads updated")
		}
	}

	return missingSet.ToSlice(), err
}

// ResolveFork создаёт merge-запись для разрешения форка
func (s *Store) ResolveFork(data []byte) (*Record, error) {
	s.mu.Lock()

	if s.heads.Len() <= 1 {
		s.mu.Unlock()
		return nil, nil // Нет форка
	}

	// Генерируем временные метки
	tid := s.clock.TID()
	s.vc.Tick(s.processID)
	vc := s.vc.Clone()

	// Все heads становятся родителями
	parents := s.sortedHeads()

	record := NewMergeRecord(s.processID, tid, vc, parents, data)
	cid := record.CID()

	err := s.db.Update(func(txn *badger.Txn) error {
		if err := s.putRecord(txn, cid, record, false); err != nil {
			return err
		}

		// Очищаем heads
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixHeads
		it := txn.NewIterator(opts)
		for it.Rewind(); it.Valid(); it.Next() {
			txn.Delete(it.Item().KeyCopy(nil))
		}
		it.Close()

		// Единственный новый head
		headKey := append(prefixHeads, cid[:]...)
		return txn.Set(headKey, nil)
	})

	if err == nil {
		s.heads = NewCIDSet(cid)
	}

	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	s.logger.Info().
		Str("cid", cid.Short()).
		Int("merged_heads", len(parents)).
		Msg("fork resolved")

	s.notify(cid, record)
	return record, nil
}

// AutoResolveFork автоматически разрешает форк детерминистически
// Без создания merge-записи, просто выбирает победителя
func (s *Store) AutoResolveFork() (CID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heads.Len() <= 1 {
		if s.heads.Len() == 1 {
			for h := range s.heads {
				return h, nil
			}
		}
		return EmptyCID(), nil
	}

	// Загружаем записи для resolver
	records := make([]*Record, 0, s.heads.Len())
	for head := range s.heads {
		r, err := s.get(head)
		if err != nil {
			continue
		}
		records = append(records, r)
	}

	winner := s.resolver.Resolve(records)
	if winner == nil {
		return EmptyCID(), nil
	}

	return winner.CID(), nil
}

// Get возвращает запись по CID
func (s *Store) Get(cid CID) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.get(cid)
}

func (s *Store) get(cid CID) (*Record, error) {
	var record *Record

	err := s.db.View(func(txn *badger.Txn) error {
		// CID → TID
		indexKey := append(prefixCIDIndex, cid[:]...)
		item, err := txn.Get(indexKey)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		var tid hlc.TID
		if err := item.Value(func(val []byte) error {
			var err error
			tid, err = hlc.TIDFromBytes(val)
			return err
		}); err != nil {
			return err
		}

		// TID → Record
		recordKey := append(prefixRecords, tid.Bytes()...)
		item, err = txn.Get(recordKey)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			record, err = UnmarshalRecord(val)
			return err
		})
	})

	return record, err
}

// GetByTID возвращает запись по TID
func (s *Store) GetByTID(tid hlc.TID) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var record *Record
	err := s.db.View(func(txn *badger.Txn) error {
		key := append(prefixRecords, tid.Bytes()...)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			record, err = UnmarshalRecord(val)
			return err
		})
	})

	return record, err
}

// Has проверяет наличие записи
func (s *Store) Has(cid CID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.has(cid)
}

func (s *Store) has(cid CID) bool {
	err := s.db.View(func(txn *badger.Txn) error {
		key := append(prefixCIDIndex, cid[:]...)
		_, err := txn.Get(key)
		return err
	})
	return err == nil
}

// Parents возвращает родителей записи
func (s *Store) Parents(cid CID) ([]CID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.parents(cid)
}

func (s *Store) parents(cid CID) ([]CID, error) {
	var parents []CID

	err := s.db.View(func(txn *badger.Txn) error {
		key := append(prefixDAG, cid[:]...)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			for i := 0; i+32 <= len(val); i += 32 {
				var c CID
				copy(c[:], val[i:i+32])
				parents = append(parents, c)
			}
			return nil
		})
	})

	return parents, err
}

// Missing возвращает отсутствующие CID из списка
func (s *Store) Missing(cids []CID) []CID {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var missing []CID
	for _, c := range cids {
		if !s.has(c) {
			missing = append(missing, c)
		}
	}
	return missing
}

// Range итерирует по записям в диапазоне TID
func (s *Store) Range(from, to hlc.TID, fn func(*Record) bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixRecords

		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := append(prefixRecords, from.Bytes()...)
		endKey := append(prefixRecords, to.Bytes()...)

		for it.Seek(startKey); it.Valid(); it.Next() {
			key := it.Item().Key()
			if bytes.Compare(key, endKey) > 0 {
				break
			}

			var record *Record
			err := it.Item().Value(func(val []byte) error {
				var err error
				record, err = UnmarshalRecord(val)
				return err
			})
			if err != nil {
				continue
			}

			if !fn(record) {
				break
			}
		}

		return nil
	})
}

// Count возвращает количество записей
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixCIDIndex
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count
}

// Stats возвращает статистику
type StoreStats struct {
	RecordCount int
	HeadCount   int
	ProcessID   string
	HasFork     bool
}

func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return StoreStats{
		RecordCount: s.Count(),
		HeadCount:   s.heads.Len(),
		ProcessID:   s.processID,
		HasFork:     s.heads.Len() > 1,
	}
}
