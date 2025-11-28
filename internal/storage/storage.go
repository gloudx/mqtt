package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/tid"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Storage интерфейс для персистентности событий
type Storage interface {
	Store(ctx context.Context, event *event.Event) error                         // Store сохраняет событие
	Load(ctx context.Context, id tid.TID) (*event.Event, error)                  // Load загружает событие по ID
	LoadRange(ctx context.Context, start, end time.Time) ([]*event.Event, error) // LoadRange загружает события в диапазоне
	LoadByDID(ctx context.Context, did *identity.DID) ([]*event.Event, error)    // LoadByDID загружает все события узла по DID
	LoadAll(ctx context.Context) ([]*event.Event, error)                         // LoadAll загружает все события
	LoadFrom(ctx context.Context, from tid.TID) ([]*event.Event, error)          // LoadFrom загружает события, начиная с указанного TID
	Delete(ctx context.Context, id tid.TID) error                                // Delete удаляет событие
	GetHeads(ctx context.Context) ([]*event.Event, error)                        // GetHead возвращает последние события (головы DAG)
	UpdateHeads(ctx context.Context, heads []tid.TID) error                      // UpdateHeads обновляет головы DAG
}

var _ Storage = (*BadgerStorage)(nil) // Проверка реализации интерфейса

// BadgerStorage реализация Storage интерфейса на базе BadgerDB
type BadgerStorage struct {
	db     *badger.DB
	prefix string
}

// NewBadgerStorage создает новый BadgerDB storage для EventLog
func NewBadgerStorage(db *badger.DB, prefix string) *BadgerStorage {
	if prefix == "" {
		prefix = "eventlog"
	}
	return &BadgerStorage{
		db:     db,
		prefix: prefix,
	}
}

// Store сохраняет событие
func (s *BadgerStorage) Store(ctx context.Context, ev *event.Event) error {
	data, err := ev.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	key := s.eventKey(ev.EventTID)

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// Load загружает событие по ID
func (s *BadgerStorage) Load(ctx context.Context, id tid.TID) (*event.Event, error) {
	var ev *event.Event
	key := s.eventKey(id)

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			ev, err = event.FromJSON(val)
			return err
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("event %s not found", id.String())
	}

	return ev, err
}

// LoadFrom загружает события, начиная с указанного TID
func (s *BadgerStorage) LoadFrom(ctx context.Context, from tid.TID) ([]*event.Event, error) {
	events := make([]*event.Event, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = s.eventPrefix()
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := s.eventKey(from)
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				ev, err := event.FromJSON(val)
				if err != nil {
					return err
				}
				events = append(events, ev)
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	return events, err
}

// LoadRange загружает события в диапазоне времени
func (s *BadgerStorage) LoadRange(ctx context.Context, start, end time.Time) ([]*event.Event, error) {
	events := make([]*event.Event, 0)

	startTID := tid.NewTIDFromTime(start, 0)
	endTID := tid.NewTIDFromTime(end, 1023)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = s.eventPrefix()
		it := txn.NewIterator(opts)
		defer it.Close()

		// Переходим к началу диапазона
		startKey := s.eventKey(startTID)
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// Проверяем, не вышли ли за пределы диапазона
			tidStr := s.extractTIDFromKey(key)
			eventTID, err := tid.ParseTID(tidStr)
			if err != nil {
				continue
			}

			// Сравниваем времена
			if eventTID.Time().After(endTID.Time()) {
				break
			}

			err = item.Value(func(val []byte) error {
				ev, err := event.FromJSON(val)
				if err != nil {
					return err
				}
				events = append(events, ev)
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	return events, err
}

// LoadByDID загружает все события узла по DID
func (s *BadgerStorage) LoadByDID(ctx context.Context, did *identity.DID) ([]*event.Event, error) {
	events := make([]*event.Event, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = s.eventPrefix()
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				ev, err := event.FromJSON(val)
				if err != nil {
					return err
				}

				// Фильтруем по DID автора
				if ev.AuthorDID == did.String() {
					events = append(events, ev)
				}
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	return events, err
}

// LoadAll загружает все события
func (s *BadgerStorage) LoadAll(ctx context.Context) ([]*event.Event, error) {
	events := make([]*event.Event, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = s.eventPrefix()
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				ev, err := event.FromJSON(val)
				if err != nil {
					return err
				}
				events = append(events, ev)
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	return events, err
}

// ItrateAll итерирует все события и применяет функцию handler
func (s *BadgerStorage) ItrateAll(ctx context.Context, handler func(*event.Event) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = s.eventPrefix()
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				ev, err := event.FromJSON(val)
				if err != nil {
					return err
				}
				return handler(ev)
			})

			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Delete удаляет событие
func (s *BadgerStorage) Delete(ctx context.Context, id tid.TID) error {
	key := s.eventKey(id)

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// GetHeads возвращает последние события (головы DAG)
func (s *BadgerStorage) GetHeads(ctx context.Context) ([]*event.Event, error) {
	var headsData []byte

	key := s.headsKey()

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // Нет heads - это нормально для нового лога
			}
			return err
		}

		return item.Value(func(val []byte) error {
			headsData = append([]byte{}, val...)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	if headsData == nil {
		return []*event.Event{}, nil
	}

	// Десериализуем список TID
	var headTIDs []string
	if err := json.Unmarshal(headsData, &headTIDs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal heads: %w", err)
	}

	// Загружаем события
	events := make([]*event.Event, 0, len(headTIDs))
	for _, tidStr := range headTIDs {
		t, err := tid.ParseTID(tidStr)
		if err != nil {
			continue
		}

		ev, err := s.Load(ctx, t)
		if err != nil {
			continue
		}
		events = append(events, ev)
	}

	return events, nil
}

// UpdateHeads обновляет головы DAG
func (s *BadgerStorage) UpdateHeads(ctx context.Context, heads []tid.TID) error {
	// Сериализуем в список строк
	headStrs := make([]string, len(heads))
	for i, h := range heads {
		headStrs[i] = h.String()
	}

	data, err := json.Marshal(headStrs)
	if err != nil {
		return fmt.Errorf("failed to marshal heads: %w", err)
	}

	key := s.headsKey()

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// Вспомогательные методы для генерации ключей

func (s *BadgerStorage) eventPrefix() []byte {
	return []byte(fmt.Sprintf("%s:event:", s.prefix))
}

func (s *BadgerStorage) eventKey(id tid.TID) []byte {
	return []byte(fmt.Sprintf("%s:event:%s", s.prefix, id.String()))
}

func (s *BadgerStorage) headsKey() []byte {
	return []byte(fmt.Sprintf("%s:heads", s.prefix))
}

func (s *BadgerStorage) extractTIDFromKey(key []byte) string {
	keyStr := string(key)
	prefix := fmt.Sprintf("%s:event:", s.prefix)
	if len(keyStr) > len(prefix) {
		return keyStr[len(prefix):]
	}
	return ""
}

// LoadByCollection загружает события по коллекции (дополнительный метод)
func (s *BadgerStorage) LoadByCollection(ctx context.Context, collection string) ([]*event.Event, error) {
	events := make([]*event.Event, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = s.eventPrefix()
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				ev, err := event.FromJSON(val)
				if err != nil {
					return err
				}

				// Фильтруем по коллекции
				if ev.Collection == collection {
					events = append(events, ev)
				}
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	return events, err
}

// Count возвращает количество событий
func (s *BadgerStorage) Count(ctx context.Context) (int, error) {
	count := 0

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = s.eventPrefix()
		opts.PrefetchValues = false // Нам не нужны значения
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count, err
}
