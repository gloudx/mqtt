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

// Export сохраняет весь лог в файл
func (s *Store) Export(path string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	// Version
	if err := binary.Write(w, binary.BigEndian, uint32(exportVersion)); err != nil {
		return err
	}

	// Собираем события
	var events []*Event
	err = s.db.View(func(txn *badger.Txn) error {
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
			events = append(events, event)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Event count
	if err := binary.Write(w, binary.BigEndian, uint32(len(events))); err != nil {
		return err
	}

	// Events
	for _, event := range events {
		data := event.Marshal()
		if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}

	// Heads count
	heads := s.sortedHeads()
	if err := binary.Write(w, binary.BigEndian, uint32(len(heads))); err != nil {
		return err
	}

	// Heads
	for _, h := range heads {
		if _, err := w.Write(h[:]); err != nil {
			return err
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}

	s.logger.Info().
		Int("events", len(events)).
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

// ExportWriter экспортирует в io.Writer
func (s *Store) ExportWriter(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bw := bufio.NewWriter(w)

	if err := binary.Write(bw, binary.BigEndian, uint32(exportVersion)); err != nil {
		return err
	}

	var events []*Event
	s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixEvents
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			it.Item().Value(func(val []byte) error {
				if event, err := UnmarshalEvent(val); err == nil {
					events = append(events, event)
				}
				return nil
			})
		}
		return nil
	})

	binary.Write(bw, binary.BigEndian, uint32(len(events)))

	for _, event := range events {
		data := event.Marshal()
		binary.Write(bw, binary.BigEndian, uint32(len(data)))
		bw.Write(data)
	}

	heads := s.sortedHeads()
	binary.Write(bw, binary.BigEndian, uint32(len(heads)))
	for _, h := range heads {
		bw.Write(h[:])
	}

	return bw.Flush()
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
