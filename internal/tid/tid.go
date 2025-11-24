package tid

import (
	"encoding/base32"
	"errors"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	Base32SortAlphabet = "234567abcdefghijklmnopqrstuvwxyz"
)

func Base32Sort() *base32.Encoding {
	return base32.NewEncoding(Base32SortAlphabet).WithPadding(base32.NoPadding)
}

type TID string

var tidRegex = regexp.MustCompile(`^[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}$`)

func ParseTID(raw string) (TID, error) {
	if raw == "" {
		return "", errors.New("expected TID, got empty string")
	}
	if len(raw) != 13 {
		return "", errors.New("TID is wrong length (expected 13 chars)")
	}
	if !tidRegex.MatchString(raw) {
		return "", errors.New("TID syntax didn't validate via regex")
	}
	return TID(raw), nil
}

func NewTIDNow(clockId uint) TID {
	return NewTID(time.Now().UTC().UnixMicro(), clockId)
}

func NewTIDFromInteger(v uint64) TID {
	v = (0x7FFF_FFFF_FFFF_FFFF & v)
	s := ""
	for i := 0; i < 13; i++ {
		s = string(Base32SortAlphabet[v&0x1F]) + s
		v = v >> 5
	}
	return TID(s)
}

func NewTID(unixMicros int64, clockId uint) TID {
	v := (uint64(unixMicros&0x1F_FFFF_FFFF_FFFF) << 10) | uint64(clockId&0x3FF)
	return NewTIDFromInteger(v)
}

func NewTIDFromTime(ts time.Time, clockId uint) TID {
	return NewTID(ts.UTC().UnixMicro(), clockId)
}

func (t TID) Integer() uint64 {
	s := t.String()
	if len(s) != 13 {
		return 0
	}
	var v uint64
	for i := 0; i < 13; i++ {
		c := strings.IndexByte(Base32SortAlphabet, s[i])
		if c < 0 {
			return 0
		}
		v = (v << 5) | uint64(c&0x1F)
	}
	return v
}

func (t TID) Time() time.Time {
	i := t.Integer()
	i = (i >> 10) & 0x1FFF_FFFF_FFFF_FFFF
	return time.UnixMicro(int64(i)).UTC()
}

func (t TID) ClockID() uint {
	i := t.Integer()
	return uint(i & 0x3FF)
}

func (t TID) String() string {
	return string(t)
}

func (t TID) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *TID) UnmarshalText(text []byte) error {
	tid, err := ParseTID(string(text))
	if err != nil {
		return err
	}
	*t = tid
	return nil
}

type TIDClock struct {
	ClockID       uint
	mtx           sync.Mutex
	lastUnixMicro int64
}

func NewTIDClock(clockId uint) *TIDClock {
	return &TIDClock{
		ClockID: clockId,
	}
}

func ClockFromTID(t TID) TIDClock {
	um := t.Integer()
	um = (um >> 10) & 0x1FFF_FFFF_FFFF_FFFF
	return TIDClock{
		ClockID:       t.ClockID(),
		lastUnixMicro: int64(um),
	}
}

func (c *TIDClock) Next() TID {
	now := time.Now().UTC().UnixMicro()
	c.mtx.Lock()
	if now <= c.lastUnixMicro {
		now = c.lastUnixMicro + 1
	}
	c.lastUnixMicro = now
	c.mtx.Unlock()
	return NewTID(now, c.ClockID)
}
