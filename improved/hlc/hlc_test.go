package hlc

import (
	"sync"
	"testing"
	"time"
)

func TestClockNow(t *testing.T) {
	clock := New("test")

	ts1 := clock.Now()
	ts2 := clock.Now()
	ts3 := clock.Now()

	if !ts1.Before(ts2) {
		t.Error("ts1 should be before ts2")
	}

	if !ts2.Before(ts3) {
		t.Error("ts2 should be before ts3")
	}
}

func TestClockUpdate(t *testing.T) {
	clock1 := New("node1")
	clock2 := New("node2")

	// События на clock1
	ts1 := clock1.Now()
	ts2 := clock1.Now()

	// clock2 получает ts2
	ts3, err := clock2.Update(ts2)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// ts3 должен быть больше ts2
	if !ts3.After(ts2) {
		t.Error("ts3 should be after ts2")
	}

	// И больше ts1
	if !ts3.After(ts1) {
		t.Error("ts3 should be after ts1")
	}
}

func TestClockDrift(t *testing.T) {
	clock := New("test").WithMaxDrift(1 * time.Second)

	// Слишком далёкое будущее
	futureTS := Timestamp{
		PT: uint64(time.Now().Add(1 * time.Hour).UnixMicro()),
		LC: 0,
	}

	_, err := clock.Update(futureTS)
	if err != ErrFuture {
		t.Errorf("expected ErrFuture, got %v", err)
	}
}

func TestTID(t *testing.T) {
	clock := New("test")

	tid1 := clock.TID()
	tid2 := clock.TID()

	if tid1 >= tid2 {
		t.Error("tid2 should be greater than tid1")
	}

	// Проверяем сериализацию
	s := tid1.String()
	if len(s) != 13 {
		t.Errorf("expected 13 chars, got %d", len(s))
	}

	parsed, err := ParseTID(s)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if parsed != tid1 {
		t.Error("parsed TID doesn't match original")
	}
}

func TestTIDBytes(t *testing.T) {
	clock := New("test")
	tid := clock.TID()

	b := tid.Bytes()
	if len(b) != 8 {
		t.Errorf("expected 8 bytes, got %d", len(b))
	}

	restored, err := TIDFromBytes(b)
	if err != nil {
		t.Fatalf("TIDFromBytes failed: %v", err)
	}

	if restored != tid {
		t.Error("restored TID doesn't match")
	}
}

func TestTimestampMarshal(t *testing.T) {
	ts := Timestamp{PT: 1234567890123456, LC: 42}

	data := ts.Marshal()
	if len(data) != 10 {
		t.Errorf("expected 10 bytes, got %d", len(data))
	}

	restored, err := UnmarshalTimestamp(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !restored.Equal(ts) {
		t.Error("restored timestamp doesn't match")
	}
}

func TestConcurrency(t *testing.T) {
	clock := New("test")
	var wg sync.WaitGroup

	tids := make(chan TID, 1000)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				tids <- clock.TID()
			}
		}()
	}

	wg.Wait()
	close(tids)

	// Проверяем уникальность
	seen := make(map[TID]struct{})
	for tid := range tids {
		if _, exists := seen[tid]; exists {
			t.Error("duplicate TID detected")
		}
		seen[tid] = struct{}{}
	}

	if len(seen) != 1000 {
		t.Errorf("expected 1000 unique TIDs, got %d", len(seen))
	}
}

func TestTIDRange(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	tidFrom, tidTo := TIDRange(from, to)

	if tidFrom >= tidTo {
		t.Error("from should be less than to")
	}

	// Проверяем что TID в диапазоне
	clock := New("test").WithTimeFunc(func() uint64 {
		return uint64(from.Add(12 * time.Hour).UnixMicro())
	})

	tid := clock.TID()

	if tid < tidFrom || tid > tidTo {
		t.Error("TID should be in range")
	}
}

func BenchmarkTID(b *testing.B) {
	clock := New("bench")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = clock.TID()
	}
}

func BenchmarkTIDString(b *testing.B) {
	clock := New("bench")
	tid := clock.TID()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = tid.String()
	}
}

func BenchmarkParseTID(b *testing.B) {
	clock := New("bench")
	s := clock.TID().String()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = ParseTID(s)
	}
}
