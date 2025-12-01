// integrity.go
package elog

import (
	"fmt"
	"mqtt-http-tunnel/internal/hlc"

	badger "github.com/dgraph-io/badger/v4"
)

// IntegrityError описывает проблему целостности
type IntegrityError struct {
	CID     CID
	Problem string
}

func (e IntegrityError) Error() string {
	return fmt.Sprintf("%s: %s", e.CID.Short(), e.Problem)
}

// IntegrityReport результат проверки
type IntegrityReport struct {
	TotalEvents    int
	ValidEvents    int
	OrphanHeads    []CID         // heads без событий
	MissingParents map[CID][]CID // event → отсутствующие parents
	CIDMismatches  []CID         // события с неверным CID
	DanglingIndex  []CID         // индексы без событий
}

func (r *IntegrityReport) IsHealthy() bool {
	return len(r.OrphanHeads) == 0 &&
		len(r.MissingParents) == 0 &&
		len(r.CIDMismatches) == 0 &&
		len(r.DanglingIndex) == 0
}

// CheckIntegrity проверяет целостность хранилища
func (s *Store) CheckIntegrity() (*IntegrityReport, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	report := &IntegrityReport{
		MissingParents: make(map[CID][]CID),
	}

	// Собираем все CID из индекса
	indexedCIDs := make(map[CID]hlc.TID)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixCIDIndex
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			var cid CID
			copy(cid[:], key[len(prefixCIDIndex):])

			var tid hlc.TID
			it.Item().Value(func(val []byte) error {
				tid, _ = hlc.TIDFromBytes(val)
				return nil
			})

			indexedCIDs[cid] = tid
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Проверяем каждое событие
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixEvents
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			report.TotalEvents++

			var event *Event
			if err := it.Item().Value(func(val []byte) error {
				var err error
				event, err = UnmarshalEvent(val)
				return err
			}); err != nil {
				continue
			}

			cid := event.CID()

			// Проверяем CID в индексе
			if _, ok := indexedCIDs[cid]; !ok {
				report.DanglingIndex = append(report.DanglingIndex, cid)
				continue
			}

			// Проверяем parents
			var missing []CID
			for _, p := range event.Parents {
				if _, ok := indexedCIDs[p]; !ok {
					missing = append(missing, p)
				}
			}
			if len(missing) > 0 {
				report.MissingParents[cid] = missing
				continue
			}

			report.ValidEvents++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Проверяем heads
	for head := range s.heads {
		if _, ok := indexedCIDs[head]; !ok {
			report.OrphanHeads = append(report.OrphanHeads, head)
		}
	}

	return report, nil
}

// Repair пытается исправить проблемы
func (s *Store) Repair() error {
	report, err := s.CheckIntegrity()
	if err != nil {
		return err
	}

	if report.IsHealthy() {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Удаляем orphan heads
	for _, h := range report.OrphanHeads {
		delete(s.heads, h)
	}

	// Сохраняем исправленные heads
	if len(report.OrphanHeads) > 0 {
		if err := s.db.Update(func(txn *badger.Txn) error {
			return s.saveHeads(txn)
		}); err != nil {
			return err
		}
	}

	s.logger.Info().
		Int("orphan_heads_removed", len(report.OrphanHeads)).
		Int("missing_parents", len(report.MissingParents)).
		Msg("repair completed")

	return nil
}
