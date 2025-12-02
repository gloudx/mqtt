package evstore

import (
	"testing"

	"mqtt-http-tunnel/improved/hlc"
	"mqtt-http-tunnel/improved/vclock"
)

func TestMergeResolverLWW(t *testing.T) {
	resolver := NewMergeResolver(MergeStrategyLWW)
	clock := hlc.New("test")

	// Создаём записи с разными TID
	r1 := NewRecord("p1", clock.TID(), nil, nil, []byte("1"))
	r2 := NewRecord("p2", clock.TID(), nil, nil, []byte("2"))
	r3 := NewRecord("p3", clock.TID(), nil, nil, []byte("3"))

	// Последний (r3) должен победить
	winner := resolver.Resolve([]*Record{r1, r2, r3})

	if winner.TID != r3.TID {
		t.Error("LWW: latest TID should win")
	}
}

func TestMergeResolverLexical(t *testing.T) {
	resolver := NewMergeResolver(MergeStrategyLexical)
	clock := hlc.New("test")

	r1 := NewRecord("p1", clock.TID(), nil, nil, []byte("aaa"))
	r2 := NewRecord("p2", clock.TID(), nil, nil, []byte("bbb"))
	r3 := NewRecord("p3", clock.TID(), nil, nil, []byte("ccc"))

	// Результат должен быть детерминистичен
	winner1 := resolver.Resolve([]*Record{r1, r2, r3})
	winner2 := resolver.Resolve([]*Record{r3, r1, r2})
	winner3 := resolver.Resolve([]*Record{r2, r3, r1})

	if !winner1.CID().Equal(winner2.CID()) || !winner2.CID().Equal(winner3.CID()) {
		t.Error("Lexical: result should be deterministic regardless of order")
	}
}

func TestMergeResolverVC(t *testing.T) {
	resolver := NewMergeResolver(MergeStrategyVC)
	clock := hlc.New("test")

	// r1 ≺ r3 (r3 знает о r1)
	vc1 := vclock.FromMap(map[string]uint64{"p1": 1})
	vc2 := vclock.FromMap(map[string]uint64{"p2": 1})
	vc3 := vclock.FromMap(map[string]uint64{"p1": 1, "p2": 1, "p3": 1}) // После обоих

	r1 := NewRecord("p1", clock.TID(), vc1, nil, []byte("1"))
	r2 := NewRecord("p2", clock.TID(), vc2, nil, []byte("2"))
	r3 := NewRecord("p3", clock.TID(), vc3, nil, []byte("3"))

	winner := resolver.Resolve([]*Record{r1, r2, r3})

	// r3 должен победить т.к. он "после" остальных по VC
	if !winner.CID().Equal(r3.CID()) {
		t.Error("VC: record that happened after should win")
	}
}

func TestMergeResolverVCConcurrent(t *testing.T) {
	resolver := NewMergeResolver(MergeStrategyVC)
	clock := hlc.New("test")

	// Все параллельны - fallback на lexical
	vc1 := vclock.FromMap(map[string]uint64{"p1": 1})
	vc2 := vclock.FromMap(map[string]uint64{"p2": 1})
	vc3 := vclock.FromMap(map[string]uint64{"p3": 1})

	r1 := NewRecord("p1", clock.TID(), vc1, nil, []byte("1"))
	r2 := NewRecord("p2", clock.TID(), vc2, nil, []byte("2"))
	r3 := NewRecord("p3", clock.TID(), vc3, nil, []byte("3"))

	// Должен быть детерминистичен
	winner1 := resolver.Resolve([]*Record{r1, r2, r3})
	winner2 := resolver.Resolve([]*Record{r3, r1, r2})

	if !winner1.CID().Equal(winner2.CID()) {
		t.Error("VC with concurrent events should fallback to deterministic")
	}
}

func TestMergeResolverCustom(t *testing.T) {
	// Кастомная стратегия: выбираем запись с минимальным TID
	resolver := NewCustomMergeResolver(func(records []*Record) *Record {
		var min *Record
		for _, r := range records {
			if min == nil || r.TID < min.TID {
				min = r
			}
		}
		return min
	})

	clock := hlc.New("test")

	r1 := NewRecord("p1", clock.TID(), nil, nil, []byte("1"))
	r2 := NewRecord("p2", clock.TID(), nil, nil, []byte("2"))
	r3 := NewRecord("p3", clock.TID(), nil, nil, []byte("3"))

	winner := resolver.Resolve([]*Record{r2, r3, r1})

	if !winner.CID().Equal(r1.CID()) {
		t.Error("Custom: earliest TID should win")
	}
}

func TestResolveCIDsLexical(t *testing.T) {
	cid1 := CIDFromData([]byte("aaa"))
	cid2 := CIDFromData([]byte("bbb"))
	cid3 := CIDFromData([]byte("ccc"))

	// Детерминистичность
	winner1 := ResolveCIDsLexical([]CID{cid1, cid2, cid3})
	winner2 := ResolveCIDsLexical([]CID{cid3, cid1, cid2})
	winner3 := ResolveCIDsLexical([]CID{cid2, cid3, cid1})

	if !winner1.Equal(winner2) || !winner2.Equal(winner3) {
		t.Error("ResolveCIDsLexical should be deterministic")
	}
}

func TestIsConcurrentSet(t *testing.T) {
	clock := hlc.New("test")

	// Все параллельны
	vc1 := vclock.FromMap(map[string]uint64{"p1": 1})
	vc2 := vclock.FromMap(map[string]uint64{"p2": 1})
	vc3 := vclock.FromMap(map[string]uint64{"p3": 1})

	r1 := NewRecord("p1", clock.TID(), vc1, nil, nil)
	r2 := NewRecord("p2", clock.TID(), vc2, nil, nil)
	r3 := NewRecord("p3", clock.TID(), vc3, nil, nil)

	if !IsConcurrentSet([]*Record{r1, r2, r3}) {
		t.Error("all should be concurrent")
	}

	// Добавляем не-параллельную
	vc4 := vclock.FromMap(map[string]uint64{"p1": 2}) // После r1
	r4 := NewRecord("p1", clock.TID(), vc4, nil, nil)

	if IsConcurrentSet([]*Record{r1, r4}) {
		t.Error("r1 and r4 should not be concurrent")
	}
}

func TestMergeResolverEmpty(t *testing.T) {
	resolver := NewMergeResolver(MergeStrategyLWW)

	if resolver.Resolve(nil) != nil {
		t.Error("empty slice should return nil")
	}

	if resolver.Resolve([]*Record{}) != nil {
		t.Error("empty slice should return nil")
	}
}

func TestMergeResolverSingle(t *testing.T) {
	resolver := NewMergeResolver(MergeStrategyLWW)
	clock := hlc.New("test")

	r := NewRecord("p1", clock.TID(), nil, nil, []byte("only one"))

	winner := resolver.Resolve([]*Record{r})
	if !winner.CID().Equal(r.CID()) {
		t.Error("single record should return itself")
	}
}

func BenchmarkMergeResolverLWW(b *testing.B) {
	resolver := NewMergeResolver(MergeStrategyLWW)
	clock := hlc.New("bench")

	records := make([]*Record, 10)
	for i := range records {
		records[i] = NewRecord("p", clock.TID(), nil, nil, []byte{byte(i)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = resolver.Resolve(records)
	}
}

func BenchmarkMergeResolverVC(b *testing.B) {
	resolver := NewMergeResolver(MergeStrategyVC)
	clock := hlc.New("bench")

	records := make([]*Record, 10)
	for i := range records {
		vc := vclock.FromMap(map[string]uint64{
			"p1": uint64(i),
			"p2": uint64(10 - i),
		})
		records[i] = NewRecord("p", clock.TID(), vc, nil, []byte{byte(i)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = resolver.Resolve(records)
	}
}
