package evstore

import (
	"bytes"
	"sort"

	badger "github.com/dgraph-io/badger/v4"
)

// Walk обходит DAG топологически (от genesis к heads)
// Использует streaming подход - загружает только метаданные DAG,
// а события читает по мере необходимости
func (s *Store) Walk(fn func(cid CID, event *Event) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Шаг 1: Собираем только метаданные DAG (CID → Parents)
	// Это ~40 байт на событие вместо ~1KB+
	type dagNode struct {
		parents []CID
		tid     hlc.TID
	}
	dag := make(map[CID]*dagNode)
	children := make(map[CID][]CID) // parent → children

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Только ключи на первом проходе
		opts.Prefix = prefixDAG
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			// Извлекаем CID из ключа d/{CID}
			if len(key) < len(prefixDAG)+32 {
				continue
			}
			var cid CID
			copy(cid[:], key[len(prefixDAG):])

			// Читаем parents
			var parents []CID
			if err := it.Item().Value(func(val []byte) error {
				for i := 0; i+32 <= len(val); i += 32 {
					var p CID
					copy(p[:], val[i:i+32])
					parents = append(parents, p)
					children[p] = append(children[p], cid)
				}
				return nil
			}); err != nil {
				continue
			}

			// Получаем TID из индекса
			indexKey := append(prefixCIDIndex, cid[:]...)
			item, err := txn.Get(indexKey)
			if err != nil {
				continue
			}
			var tid hlc.TID
			item.Value(func(val []byte) error {
				tid, _ = hlc.TIDFromBytes(val)
				return nil
			})

			dag[cid] = &dagNode{parents: parents, tid: tid}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Шаг 2: Находим genesis (узлы без родителей)
	var genesis []CID
	for cid, node := range dag {
		if len(node.parents) == 0 {
			genesis = append(genesis, cid)
		}
	}
	sortCIDs(genesis)

	// Шаг 3: Топологическая сортировка (Kahn's algorithm)
	visited := make(map[CID]struct{})
	inDegree := make(map[CID]int)

	for cid, node := range dag {
		inDegree[cid] = len(node.parents)
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

	// Шаг 4: Читаем события по одному в правильном порядке и вызываем callback
	return s.db.View(func(txn *badger.Txn) error {
		for _, cid := range order {
			node := dag[cid]
			if node == nil {
				continue
			}

			// Читаем событие по TID
			eventKey := append(prefixEvents, node.tid.Bytes()...)
			item, err := txn.Get(eventKey)
			if err != nil {
				continue
			}

			var event *Event
			if err := item.Value(func(val []byte) error {
				var err error
				event, err = UnmarshalEvent(val)
				return err
			}); err != nil {
				continue
			}

			if err := fn(cid, event); err != nil {
				return err
			}
		}
		return nil
	})
}

// WalkFrom обходит от CID к genesis (ancestors)
// Использует streaming - сначала собирает порядок обхода,
// потом читает события по одному
func (s *Store) WalkFrom(cid CID, fn func(cid CID, event *Event) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	visited := make(map[CID]struct{})
	var order []CID

	// Рекурсивный обход для построения порядка
	var visit func(c CID) error
	visit = func(c CID) error {
		if _, ok := visited[c]; ok {
			return nil
		}
		visited[c] = struct{}{}

		// Получаем только parents из DAG (не загружаем полное событие)
		parents, err := s.parents(c)
		if err != nil {
			return nil // пропускаем отсутствующие
		}

		// Сначала обрабатываем родителей
		sortedParents := make([]CID, len(parents))
		copy(sortedParents, parents)
		sortCIDs(sortedParents)

		for _, p := range sortedParents {
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

	// Читаем события по одному в правильном порядке
	return s.db.View(func(txn *badger.Txn) error {
		for _, c := range order {
			// Получаем TID из индекса
			indexKey := append(prefixCIDIndex, c[:]...)
			item, err := txn.Get(indexKey)
			if err != nil {
				continue
			}

			var tid hlc.TID
			if err := item.Value(func(val []byte) error {
				tid, err = hlc.TIDFromBytes(val)
				return err
			}); err != nil {
				continue
			}

			// Читаем событие
			eventKey := append(prefixEvents, tid.Bytes()...)
			eventItem, err := txn.Get(eventKey)
			if err != nil {
				continue
			}

			var event *Event
			if err := eventItem.Value(func(val []byte) error {
				var err error
				event, err = UnmarshalEvent(val)
				return err
			}); err != nil {
				continue
			}

			if err := fn(c, event); err != nil {
				return err
			}
		}
		return nil
	})
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

func sortCIDs(cids []CID) {
	sort.Slice(cids, func(i, j int) bool {
		return bytes.Compare(cids[i][:], cids[j][:]) < 0
	})
}
