package evstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"mqtt-http-tunnel/internal/hlc"
	"sort"
)

var (
	ErrInvalidRecord = errors.New("invalid record")
)

type Record struct {
	TID     hlc.TID
	Parents []CID
	Data    []byte
}

func (e *Record) CID() CID {
	h := sha256.New()
	binary.Write(h, binary.BigEndian, uint64(e.TID))
	parents := make([]CID, len(e.Parents))
	copy(parents, e.Parents)
	sort.Slice(parents, func(i, j int) bool {
		return bytes.Compare(parents[i][:], parents[j][:]) < 0
	})
	binary.Write(h, binary.BigEndian, uint32(len(parents)))
	for _, p := range parents {
		h.Write(p[:])
	}
	binary.Write(h, binary.BigEndian, uint32(len(e.Data)))
	h.Write(e.Data)
	var c CID
	copy(c[:], h.Sum(nil))
	return c
}

func (e *Record) Marshal() []byte {
	size := 8 + 4 + len(e.Parents)*32 + 4 + len(e.Data)
	buf := make([]byte, size)
	offset := 0
	// TID
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.TID))
	offset += 8
	// Parents
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(e.Parents)))
	offset += 4
	for _, p := range e.Parents {
		copy(buf[offset:], p[:])
		offset += 32
	}
	// Data
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(e.Data)))
	offset += 4
	copy(buf[offset:], e.Data)
	return buf
}

func UnmarshalEvent(b []byte) (*Record, error) {
	if len(b) < 16 {
		return nil, ErrInvalidRecord
	}
	offset := 0
	// TID
	tid := hlc.TID(binary.BigEndian.Uint64(b[offset:]))
	offset += 8
	// Parents
	parentsCount := binary.BigEndian.Uint32(b[offset:])
	offset += 4
	if len(b) < offset+int(parentsCount)*32+4 {
		return nil, ErrInvalidRecord
	}
	parents := make([]CID, parentsCount)
	for i := range parents {
		copy(parents[i][:], b[offset:])
		offset += 32
	}
	// Data
	dataLen := binary.BigEndian.Uint32(b[offset:])
	offset += 4
	if len(b) < offset+int(dataLen) {
		return nil, ErrInvalidRecord
	}
	data := make([]byte, dataLen)
	copy(data, b[offset:])
	return &Record{
		TID:     tid,
		Parents: parents,
		Data:    data,
	}, nil
}
