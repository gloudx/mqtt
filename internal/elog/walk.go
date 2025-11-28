// walk.go
package elog

import (
	"bytes"
	"mqtt-http-tunnel/internal/hlc"
	"sort"

	badger "github.com/dgraph-io/badger/v4"
)

// Walk обходит DAG топологически (от genesis к heads)
func (s *Store) Walk(fn func(cid CID, event *Event) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Собираем все события
	events := make(map[CID]*Event)
	children := make(map[CID][]CID) // parent → children

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixEvents
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			var event *Event
			if err := it.Item().Value(func(val []byte) error {
				var err error
				event, err = UnmarshalEvent(val)
				return err
			}); err != nil {
				continue
			}

			cid := event.CID()
			events[cid] = event

			for _, p := range event.Parents {
				children[p] = append(children[p], cid)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Находим genesis (узлы без родителей)
	var genesis []CID
	for cid, event := range events {
		if len(event.Parents) == 0 {
			genesis = append(genesis, cid)
		}
	}
	sortCIDs(genesis)

	// BFS от genesis
	visited := make(map[CID]struct{})
	inDegree := make(map[CID]int)

	for cid, event := range events {
		inDegree[cid] = len(event.Parents)
	}

	queue := genesis
	var order []CID

	for len(queue) > 0 {
		cid := queue[0]
		queue = queue[1:]

		if _, ok := visited[cid]; ok {
			continue
		}
		visited[cid] = struct{}{}
		order = append(order, cid)

		kids := children[cid]
		sortCIDs(kids)

		for _, child := range kids {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	// Вызываем callback
	for _, cid := range order {
		if err := fn(cid, events[cid]); err != nil {
			return err
		}
	}

	return nil
}

// WalkFrom обходит от CID к genesis (ancestors)
func (s *Store) WalkFrom(cid CID, fn func(cid CID, event *Event) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	visited := make(map[CID]struct{})
	var order []CID

	var visit func(c CID) error
	visit = func(c CID) error {
		if _, ok := visited[c]; ok {
			return nil
		}
		visited[c] = struct{}{}

		event, err := s.get(c)
		if err != nil {
			return nil // пропускаем отсутствующие
		}

		// Сначала родители
		parents := make([]CID, len(event.Parents))
		copy(parents, event.Parents)
		sortCIDs(parents)

		for _, p := range parents {
			if err := visit(p); err != nil {
				return err
			}
		}

		order = append(order, c)
		return nil
	}

	if err := visit(cid); err != nil {
		return err
	}

	// Callback в хронологическом порядке
	for _, c := range order {
		event, _ := s.get(c)
		if err := fn(c, event); err != nil {
			return err
		}
	}

	return nil
}

// WalkHeads обходит только текущие heads
func (s *Store) WalkHeads(fn func(cid CID, event *Event) error) error {
	heads := s.Heads()
	for _, cid := range heads {
		event, err := s.Get(cid)
		if err != nil {
			continue
		}
		if err := fn(cid, event); err != nil {
			return err
		}
	}
	return nil
}

// get без блокировки (внутренний)
func (s *Store) get(cid CID) (*Event, error) {
	var event *Event

	err := s.db.View(func(txn *badger.Txn) error {
		indexKey := append(prefixCIDIndex, cid[:]...)
		item, err := txn.Get(indexKey)
		if err != nil {
			return err
		}

		var tid hlc.TID
		if err := item.Value(func(val []byte) error {
			tid, _ = hlc.TIDFromBytes(val)
			return nil
		}); err != nil {
			return err
		}

		eventKey := append(prefixEvents, tid.Bytes()...)
		item, err = txn.Get(eventKey)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			event, err = UnmarshalEvent(val)
			return err
		})
	})

	return event, err
}

func sortCIDs(cids []CID) {
	sort.Slice(cids, func(i, j int) bool {
		return bytes.Compare(cids[i][:], cids[j][:]) < 0
	})
}
