package evstore

import "errors"

// CID - 32 байта SHA256
type CID [32]byte

func (c CID) String() string {
	const hex = "0123456789abcdef"
	buf := make([]byte, 64)
	for i, b := range c {
		buf[i*2] = hex[b>>4]
		buf[i*2+1] = hex[b&0x0f]
	}
	return string(buf)
}

func (c CID) Short() string {
	return c.String()[:8]
}

func (c CID) Bytes() []byte {
	return c[:]
}

func (c CID) IsZero() bool {
	return c == CID{}
}

func CIDFromBytes(b []byte) (CID, error) {
	if len(b) != 32 {
		return CID{}, errors.New("invalid cid length")
	}
	var c CID
	copy(c[:], b)
	return c, nil
}
