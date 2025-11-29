package evstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	badger "github.com/dgraph-io/badger/v4"
)

// Export format:
// [4 bytes: version]
// [4 bytes: event count]
// For each event:
//   [4 bytes: event length]
//   [N bytes: event data]
// [4 bytes: heads count]
// For each head:
//   [32 bytes: CID]

const exportVersion = 1

// Export сохраняет весь лог в файл (streaming)
func (s *Store) Export(path string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	// Version
	if err := binary.Write(w, binary.BigEndian, uint32(exportVersion)); err != nil {
		return err
	}

	// Подсчитываем события (первый проход)
	var eventCount uint32
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefixEvents
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			eventCount++
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Event count
	if err := binary.Write(w, binary.BigEndian, eventCount); err != nil {
		return err
	}

	// Events (второй проход - streaming write)
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixEvents
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(val []byte) error {
				// Записываем напрямую, не создавая Event объект
				if err := binary.Write(w, binary.BigEndian, uint32(len(val))); err != nil {
					return err
				}
				_, err := w.Write(val)
				return err
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Heads
	heads := s.sortedHeads()
	if err := binary.Write(w, binary.BigEndian, uint32(len(heads))); err != nil {
		return err
	}

	for _, h := range heads {
		if _, err := w.Write(h[:]); err != nil {
			return err
		}
	}

	s.logger.Info().
		Uint32("events", eventCount).
		Int("heads", len(heads)).
		Str("path", path).
		Msg("exported")

	return nil
}

// Import загружает лог из файла
func (s *Store) Import(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	// Version
	var version uint32
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return err
	}
	if version != exportVersion {
		return fmt.Errorf("unsupported export version: %d", version)
	}

	// Event count
	var eventCount uint32
	if err := binary.Read(r, binary.BigEndian, &eventCount); err != nil {
		return err
	}

	// Events
	imported := 0
	for i := uint32(0); i < eventCount; i++ {
		var dataLen uint32
		if err := binary.Read(r, binary.BigEndian, &dataLen); err != nil {
			return err
		}

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}

		event, err := UnmarshalEvent(data)
		if err != nil {
			continue
		}

		if _, err := s.Put(event); err == nil {
			imported++
		}
	}

	// Heads count
	var headsCount uint32
	if err := binary.Read(r, binary.BigEndian, &headsCount); err != nil {
		return err
	}

	// Heads
	var heads []CID
	for i := uint32(0); i < headsCount; i++ {
		var cid CID
		if _, err := io.ReadFull(r, cid[:]); err != nil {
			return err
		}
		heads = append(heads, cid)
	}

	// Merge heads
	if err := s.Merge(heads); err != nil {
		return err
	}

	s.logger.Info().
		Int("imported", imported).
		Int("heads", len(heads)).
		Str("path", path).
		Msg("imported")

	return nil
}

// ExportWriter экспортирует в io.Writer (streaming)
func (s *Store) ExportWriter(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bw := bufio.NewWriter(w)
	defer bw.Flush()

	if err := binary.Write(bw, binary.BigEndian, uint32(exportVersion)); err != nil {
		return err
	}

	// Подсчет событий
	var eventCount uint32
	if err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefixEvents
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			eventCount++
		}
		return nil
	}); err != nil {
		return err
	}

	if err := binary.Write(bw, binary.BigEndian, eventCount); err != nil {
		return err
	}

	// Streaming write событий
	if err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixEvents
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(val []byte) error {
				if err := binary.Write(bw, binary.BigEndian, uint32(len(val))); err != nil {
					return err
				}
				_, err := bw.Write(val)
				return err
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Heads
	heads := s.sortedHeads()
	if err := binary.Write(bw, binary.BigEndian, uint32(len(heads))); err != nil {
		return err
	}
	for _, h := range heads {
		if _, err := bw.Write(h[:]); err != nil {
			return err
		}
	}

	return nil
}

// ImportReader импортирует из io.Reader
func (s *Store) ImportReader(r io.Reader) error {
	br := bufio.NewReader(r)

	var version uint32
	if err := binary.Read(br, binary.BigEndian, &version); err != nil {
		return err
	}
	if version != exportVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	var eventCount uint32
	binary.Read(br, binary.BigEndian, &eventCount)

	for i := uint32(0); i < eventCount; i++ {
		var dataLen uint32
		binary.Read(br, binary.BigEndian, &dataLen)

		data := make([]byte, dataLen)
		io.ReadFull(br, data)

		if event, err := UnmarshalEvent(data); err == nil {
			s.Put(event)
		}
	}

	var headsCount uint32
	binary.Read(br, binary.BigEndian, &headsCount)

	var heads []CID
	for i := uint32(0); i < headsCount; i++ {
		var cid CID
		io.ReadFull(br, cid[:])
		heads = append(heads, cid)
	}

	return s.Merge(heads)
}
