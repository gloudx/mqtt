package evstore

import (
	"bytes"
	"errors"
	"mqtt-http-tunnel/internal/hlc"
	"sort"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
)

var ErrNotFound = errors.New("not found")

var (
	prefixRecords  = []byte("r/") // e/{TID} → Record
	prefixDAG      = []byte("d/") // d/{CID} → []CID (parents)
	prefixCIDIndex = []byte("i/") // i/{CID} → TID
	keyHeads       = []byte("meta/heads")
	keyHLC         = []byte("meta/hlc")
)

type EventHandler func(CID, *Record)

type Store struct {
	db          *badger.DB
	clock       *hlc.Clock
	heads       map[CID]struct{}
	subscribers []EventHandler
	//
	mu     sync.RWMutex
	logger zerolog.Logger
}

type StoreConfig struct {
	Path   string
	Logger zerolog.Logger
}

func OpenStore(cfg *StoreConfig) (*Store, error) {
	opts := badger.DefaultOptions(cfg.Path).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	clock := hlc.New()
	s := &Store{
		db:     db,
		clock:  clock,
		heads:  make(map[CID]struct{}),
		logger: cfg.Logger.With().Str("component", "store").Logger(),
	}
	if err := s.loadState(); err != nil {
		db.Close()
		return nil, err
	}
	s.logger.Info().Int("heads", len(s.heads)).Msg("store opened")
	return s, nil
}

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

func (s *Store) notifySubscribers(cid CID, event *Record) {
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
			h(cid, event)
		}()
	}
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) loadState() error {
	return s.db.View(func(txn *badger.Txn) error {
		// Load HLC
		if item, err := txn.Get(keyHLC); err == nil {
			item.Value(func(val []byte) error {
				if ts, err := hlc.UnmarshalTimestamp(val); err == nil {
					s.clock.Set(ts)
				}
				return nil
			})
		}
		// Load Heads
		if item, err := txn.Get(keyHeads); err == nil {
			item.Value(func(val []byte) error {
				for i := 0; i+32 <= len(val); i += 32 {
					var c CID
					copy(c[:], val[i:i+32])
					s.heads[c] = struct{}{}
				}
				return nil
			})
		}
		return nil
	})
}

func (s *Store) saveHeads(txn *badger.Txn) error {
	sorted := s.sortedHeads()
	buf := make([]byte, len(sorted)*32)
	for i, c := range sorted {
		copy(buf[i*32:], c[:])
	}
	return txn.Set(keyHeads, buf)
}

func (s *Store) saveHLC(txn *badger.Txn) error {
	return txn.Set(keyHLC, s.clock.Timestamp().Marshal())
}

func (s *Store) Heads() []CID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sortedHeads()
}

func (s *Store) sortedHeads() []CID {
	result := make([]CID, 0, len(s.heads))
	for h := range s.heads {
		result = append(result, h)
	}
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i][:], result[j][:]) < 0
	})
	return result
}

func (s *Store) Append(data []byte) (*Record, error) {
	s.mu.Lock()
	tid := s.clock.TID()
	parents := s.sortedHeads()
	r := &Record{
		TID:     tid,
		Parents: parents,
		Data:    data,
	}
	cid := r.CID()
	err := s.db.Update(func(txn *badger.Txn) error {
		// TID
		eventKey := append(prefixRecords, tid.Bytes()...)
		if err := txn.Set(eventKey, r.Marshal()); err != nil {
			return err
		}
		// DAG
		dagVal := make([]byte, len(parents)*32)
		for i, p := range parents {
			copy(dagVal[i*32:], p[:])
		}
		dagKey := append(prefixDAG, cid[:]...)
		if err := txn.Set(dagKey, dagVal); err != nil {
			return err
		}
		// CID Index
		indexKey := append(prefixCIDIndex, cid[:]...)
		if err := txn.Set(indexKey, tid.Bytes()); err != nil {
			return err
		}
		// Heads
		s.heads[cid] = struct{}{}
		for _, p := range parents {
			delete(s.heads, p)
		}
		if err := s.saveHeads(txn); err != nil {
			return err
		}
		// HLC
		return s.saveHLC(txn)
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
	s.notifySubscribers(cid, r)
	return r, nil
}

func (s *Store) Put(event *Record) (CID, error) {
	s.mu.Lock()
	cid := event.CID()
	if s.has(cid) {
		s.mu.Unlock()
		return cid, nil
	}
	s.clock.UpdateTID(event.TID)
	err := s.db.Update(func(txn *badger.Txn) error {
		// TID
		eventKey := append(prefixRecords, event.TID.Bytes()...)
		if err := txn.Set(eventKey, event.Marshal()); err != nil {
			return err
		}
		// DAG
		dagVal := make([]byte, len(event.Parents)*32)
		for i, p := range event.Parents {
			copy(dagVal[i*32:], p[:])
		}
		dagKey := append(prefixDAG, cid[:]...)
		if err := txn.Set(dagKey, dagVal); err != nil {
			return err
		}
		// CID Index
		indexKey := append(prefixCIDIndex, cid[:]...)
		if err := txn.Set(indexKey, event.TID.Bytes()); err != nil {
			return err
		}
		// HLC
		return s.saveHLC(txn)
	})
	s.mu.Unlock()
	if err != nil {
		return CID{}, err
	}
	s.logger.Debug().
		Str("cid", cid.Short()).
		Str("tid", event.TID.String()).
		Msg("put")
	return cid, nil
}

func (s *Store) Merge(cids []CID) ([]CID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	var missing []CID
	for _, c := range cids {
		if !s.has(c) {
			continue
		}
		parents, err := s.parents(c)
		if err != nil {
			s.logger.Error().Err(err).Str("cid", c.Short()).Msg("failed to get parents")
			continue
		}
		missing = append(missing, s.missing(parents)...)
		if _, exists := s.heads[c]; !exists {
			s.heads[c] = struct{}{}
			changed = true
		}
		for _, p := range parents {
			if _, exists := s.heads[p]; exists {
				delete(s.heads, p)
				changed = true
			}
		}
	}
	if !changed {
		return missing, nil
	}
	err := s.db.Update(func(txn *badger.Txn) error {
		return s.saveHeads(txn)
	})
	if err == nil {
		s.logger.Debug().
			Int("heads", len(s.heads)).
			Msg("heads updated")
	}
	return missing, err
}

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

func (s *Store) Get(cid CID) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.get(cid)
}

func (s *Store) get(cid CID) (*Record, error) {
	var r *Record
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
		// TID → Event
		eventKey := append(prefixRecords, tid.Bytes()...)
		item, err = txn.Get(eventKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			r, err = UnmarshalEvent(val)
			return err
		})
	})
	return r, err
}

func (s *Store) GetByTID(tid hlc.TID) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var r *Record
	err := s.db.View(func(txn *badger.Txn) error {
		eventKey := append(prefixRecords, tid.Bytes()...)
		item, err := txn.Get(eventKey)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			r, err = UnmarshalEvent(val)
			return err
		})
	})
	return r, err
}

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

func (s *Store) Missing(cids []CID) []CID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.missing(cids)
}

func (s *Store) missing(cids []CID) []CID {
	var missing []CID
	for _, c := range cids {
		if !s.has(c) {
			missing = append(missing, c)
		}
	}
	return missing
}

func (s *Store) Clock() *hlc.Clock {
	return s.clock
}

func (s *Store) HasFork() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.heads) > 1
}

func (s *Store) ResolveFork(data []byte) (*Record, error) {
	s.mu.Lock()
	if len(s.heads) <= 1 {
		s.mu.Unlock()
		return nil, nil // Нет форка
	}
	tid := s.clock.TID()
	parents := s.sortedHeads()
	event := &Record{
		TID:     tid,
		Parents: parents,
		Data:    data,
	}
	cid := event.CID()
	err := s.db.Update(func(txn *badger.Txn) error {
		// TID
		eventKey := append(prefixRecords, tid.Bytes()...)
		if err := txn.Set(eventKey, event.Marshal()); err != nil {
			return err
		}
		// DAG
		dagVal := make([]byte, len(parents)*32)
		for i, p := range parents {
			copy(dagVal[i*32:], p[:])
		}
		dagKey := append(prefixDAG, cid[:]...)
		if err := txn.Set(dagKey, dagVal); err != nil {
			return err
		}
		// CID Index
		indexKey := append(prefixCIDIndex, cid[:]...)
		if err := txn.Set(indexKey, tid.Bytes()); err != nil {
			return err
		}
		// Heads
		s.heads = map[CID]struct{}{cid: {}}
		if err := s.saveHeads(txn); err != nil {
			return err
		}
		// HLC
		return s.saveHLC(txn)
	})
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.logger.Info().
		Str("cid", cid.Short()).
		Str("tid", tid.String()).
		Int("merged_heads", len(parents)).
		Msg("fork resolved")
	s.notifySubscribers(cid, event)
	return event, nil
}

func (s *Store) WalkAll() error {
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Только ключи на первом проходе
		opts.Prefix = prefixDAG
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			var cid CID
			copy(cid[:], key[len(prefixDAG):])
			s.logger.Info().Str("cid", cid.String()).Msg("DAG entry")
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
