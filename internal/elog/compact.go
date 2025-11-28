// compact.go
package elog

import badger "github.com/dgraph-io/badger/v4"

// CompactHeads удаляет из heads те, которые являются ancestors других heads
func (s *Store) CompactHeads() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.heads) <= 1 {
		return nil
	}

	heads := s.sortedHeads()

	// Собираем всех ancestors для каждого head
	ancestors := make(map[CID]map[CID]struct{})

	for _, head := range heads {
		ancestors[head] = make(map[CID]struct{})
		s.collectAncestors(head, ancestors[head])
	}

	// Удаляем heads которые являются ancestors других
	newHeads := make(map[CID]struct{})

	for _, head := range heads {
		isAncestor := false
		for _, other := range heads {
			if head == other {
				continue
			}
			if _, ok := ancestors[other][head]; ok {
				isAncestor = true
				break
			}
		}
		if !isAncestor {
			newHeads[head] = struct{}{}
		}
	}

	if len(newHeads) == len(s.heads) {
		return nil // ничего не изменилось
	}

	removed := len(s.heads) - len(newHeads)
	s.heads = newHeads

	err := s.db.Update(func(txn *badger.Txn) error {
		return s.saveHeads(txn)
	})

	if err == nil {
		s.logger.Debug().
			Int("removed", removed).
			Int("remaining", len(newHeads)).
			Msg("compacted heads")
	}

	return err
}

func (s *Store) collectAncestors(cid CID, result map[CID]struct{}) {
	parents, err := s.parents(cid)
	if err != nil {
		return
	}

	for _, p := range parents {
		if _, ok := result[p]; ok {
			continue
		}
		result[p] = struct{}{}
		s.collectAncestors(p, result)
	}
}

// parents без блокировки
func (s *Store) parents(cid CID) ([]CID, error) {
	var parents []CID

	err := s.db.View(func(txn *badger.Txn) error {
		key := append(prefixDAG, cid[:]...)
		item, err := txn.Get(key)
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

// AutoCompact вызывает компакцию если heads > threshold
func (s *Store) AutoCompact(threshold int) error {
	if len(s.Heads()) > threshold {
		return s.CompactHeads()
	}
	return nil
}
