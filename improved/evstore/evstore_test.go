package evstore

import (
	"testing"

	"mqtt-http-tunnel/improved/hlc"
	"mqtt-http-tunnel/improved/vclock"
)

func TestCIDFromData(t *testing.T) {
	data := []byte("hello world")
	cid1 := CIDFromData(data)
	cid2 := CIDFromData(data)

	if !cid1.Equal(cid2) {
		t.Error("same data should produce same CID")
	}

	cid3 := CIDFromData([]byte("different"))
	if cid1.Equal(cid3) {
		t.Error("different data should produce different CID")
	}
}

func TestCIDFromHex(t *testing.T) {
	cid := CIDFromData([]byte("test"))
	hex := cid.String()

	restored, err := CIDFromHex(hex)
	if err != nil {
		t.Fatalf("CIDFromHex failed: %v", err)
	}

	if !cid.Equal(restored) {
		t.Error("restored CID doesn't match")
	}
}

func TestCIDCompare(t *testing.T) {
	cid1 := CIDFromData([]byte("aaa"))
	cid2 := CIDFromData([]byte("bbb"))

	if cid1.Compare(cid1) != 0 {
		t.Error("same CID should compare equal")
	}

	cmp := cid1.Compare(cid2)
	if cmp == 0 {
		t.Error("different CIDs should not compare equal")
	}

	// Проверяем антисимметрию
	if cid1.Compare(cid2) == cid2.Compare(cid1) {
		if cid1.Compare(cid2) != 0 {
			t.Error("compare should be antisymmetric")
		}
	}
}

func TestCIDSet(t *testing.T) {
	cid1 := CIDFromData([]byte("1"))
	cid2 := CIDFromData([]byte("2"))
	cid3 := CIDFromData([]byte("3"))

	set := NewCIDSet(cid1, cid2)

	if !set.Has(cid1) {
		t.Error("set should have cid1")
	}

	if set.Has(cid3) {
		t.Error("set should not have cid3")
	}

	set.Add(cid3)
	if !set.Has(cid3) {
		t.Error("set should have cid3 after add")
	}

	set.Remove(cid1)
	if set.Has(cid1) {
		t.Error("set should not have cid1 after remove")
	}

	if set.Len() != 2 {
		t.Errorf("expected len 2, got %d", set.Len())
	}
}

func TestCIDSetOperations(t *testing.T) {
	cid1 := CIDFromData([]byte("1"))
	cid2 := CIDFromData([]byte("2"))
	cid3 := CIDFromData([]byte("3"))

	set1 := NewCIDSet(cid1, cid2)
	set2 := NewCIDSet(cid2, cid3)

	// Union
	union := set1.Union(set2)
	if union.Len() != 3 {
		t.Errorf("union len: expected 3, got %d", union.Len())
	}

	// Intersection
	inter := set1.Intersection(set2)
	if inter.Len() != 1 || !inter.Has(cid2) {
		t.Error("intersection should only have cid2")
	}

	// Difference
	diff := set1.Difference(set2)
	if diff.Len() != 1 || !diff.Has(cid1) {
		t.Error("difference should only have cid1")
	}
}

func TestRecordCID(t *testing.T) {
	clock := hlc.New("test")
	vc := vclock.New()
	vc.Tick("test")

	r1 := NewRecord("test", clock.TID(), vc, nil, []byte("data"))
	cid1 := r1.CID()

	// Такой же record должен дать такой же CID
	r2 := &Record{
		TID:       r1.TID,
		ProcessID: r1.ProcessID,
		VC:        r1.VC,
		Parents:   r1.Parents,
		Type:      r1.Type,
		Data:      r1.Data,
	}
	cid2 := r2.CID()

	if !cid1.Equal(cid2) {
		t.Error("identical records should have same CID")
	}

	// Другие данные - другой CID
	r3 := NewRecord("test", clock.TID(), vc, nil, []byte("different"))
	cid3 := r3.CID()

	if cid1.Equal(cid3) {
		t.Error("different data should produce different CID")
	}
}

func TestRecordMarshal(t *testing.T) {
	clock := hlc.New("test")
	vc := vclock.New()
	vc.Tick("test")
	vc.Tick("other")

	parent := CIDFromData([]byte("parent"))

	r := NewRecord("test-process", clock.TID(), vc, []CID{parent}, []byte("payload data"))
	r.Signature = []byte("sig")

	data := r.Marshal()

	restored, err := UnmarshalRecord(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Проверяем поля
	if restored.TID != r.TID {
		t.Error("TID mismatch")
	}

	if restored.ProcessID != r.ProcessID {
		t.Error("ProcessID mismatch")
	}

	if restored.VC == nil || !restored.VC.IsEqual(r.VC) {
		t.Error("VC mismatch")
	}

	if len(restored.Parents) != 1 || !restored.Parents[0].Equal(parent) {
		t.Error("Parents mismatch")
	}

	if string(restored.Data) != string(r.Data) {
		t.Error("Data mismatch")
	}

	if string(restored.Signature) != string(r.Signature) {
		t.Error("Signature mismatch")
	}

	// CID должен совпадать
	if !r.CID().Equal(restored.CID()) {
		t.Error("CID mismatch after unmarshal")
	}
}

func TestRecordCausality(t *testing.T) {
	// Создаём записи с VC для проверки причинности
	vc1 := vclock.FromMap(map[string]uint64{"p1": 1})
	vc2 := vclock.FromMap(map[string]uint64{"p1": 2})
	vc3 := vclock.FromMap(map[string]uint64{"p2": 1})

	clock := hlc.New("test")

	r1 := NewRecord("p1", clock.TID(), vc1, nil, []byte("1"))
	r2 := NewRecord("p1", clock.TID(), vc2, nil, []byte("2"))
	r3 := NewRecord("p2", clock.TID(), vc3, nil, []byte("3"))

	// r1 ≺ r2 (один процесс)
	if !r1.HappensBefore(r2) {
		t.Error("r1 should happen before r2")
	}

	// r1 ∥ r3 (разные процессы, нет связи)
	if !r1.IsConcurrent(r3) {
		t.Error("r1 and r3 should be concurrent")
	}

	// r2 ∥ r3
	if !r2.IsConcurrent(r3) {
		t.Error("r2 and r3 should be concurrent")
	}
}

func TestMergeRecord(t *testing.T) {
	clock := hlc.New("test")
	vc := vclock.New()

	head1 := CIDFromData([]byte("head1"))
	head2 := CIDFromData([]byte("head2"))

	merge := NewMergeRecord("test", clock.TID(), vc, []CID{head1, head2}, nil)

	if !merge.IsMerge() {
		t.Error("should be merge record")
	}

	if merge.Type != RecordTypeMerge {
		t.Error("type should be RecordTypeMerge")
	}

	if len(merge.Parents) != 2 {
		t.Error("should have 2 parents")
	}
}

func BenchmarkRecordCID(b *testing.B) {
	clock := hlc.New("bench")
	vc := vclock.New()
	r := NewRecord("bench", clock.TID(), vc, nil, []byte("benchmark data"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.CID()
	}
}

func BenchmarkRecordMarshal(b *testing.B) {
	clock := hlc.New("bench")
	vc := vclock.New()
	r := NewRecord("bench", clock.TID(), vc, nil, []byte("benchmark data"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Marshal()
	}
}

func BenchmarkRecordUnmarshal(b *testing.B) {
	clock := hlc.New("bench")
	vc := vclock.New()
	r := NewRecord("bench", clock.TID(), vc, nil, []byte("benchmark data"))
	data := r.Marshal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = UnmarshalRecord(data)
	}
}
