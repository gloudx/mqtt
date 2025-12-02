package vclock

import (
	"testing"
)

func TestVectorClockBasic(t *testing.T) {
	vc1 := New()
	vc1.Tick("p1")
	vc1.Tick("p1")

	if vc1.Get("p1") != 2 {
		t.Errorf("expected p1=2, got %d", vc1.Get("p1"))
	}

	if vc1.Get("p2") != 0 {
		t.Errorf("expected p2=0, got %d", vc1.Get("p2"))
	}
}

func TestVectorClockCompare(t *testing.T) {
	// Сценарий из лекции: два процесса p1 и p2
	// p1: a -> b -> c
	// p2: d -> e
	// сообщение от b к e

	// После события a на p1
	vca := FromMap(map[string]uint64{"p1": 1, "p2": 0})

	// После события b на p1
	vcb := FromMap(map[string]uint64{"p1": 2, "p2": 0})

	// После события d на p2
	vcd := FromMap(map[string]uint64{"p1": 0, "p2": 1})

	// После события e на p2 (получил сообщение от b)
	vce := FromMap(map[string]uint64{"p1": 2, "p2": 2})

	// a ≺ b
	if vca.Compare(vcb) != Before {
		t.Error("expected a < b")
	}

	// b ≺ e (через сообщение)
	if vcb.Compare(vce) != Before {
		t.Error("expected b < e")
	}

	// a ≺ e (транзитивность)
	if vca.Compare(vce) != Before {
		t.Error("expected a < e")
	}

	// a ∥ d (параллельны - нет причинной связи)
	if vca.Compare(vcd) != Concurrent {
		t.Errorf("expected a || d, got %v", vca.Compare(vcd))
	}

	// d ∥ b (параллельны)
	if vcd.Compare(vcb) != Concurrent {
		t.Errorf("expected d || b, got %v", vcd.Compare(vcb))
	}

	// d ≺ e
	if vcd.Compare(vce) != Before {
		t.Error("expected d < e")
	}
}

func TestVectorClockMerge(t *testing.T) {
	vc1 := FromMap(map[string]uint64{"p1": 3, "p2": 1})
	vc2 := FromMap(map[string]uint64{"p1": 1, "p2": 4, "p3": 2})

	vc1.Merge(vc2)

	expected := map[string]uint64{"p1": 3, "p2": 4, "p3": 2}
	for k, v := range expected {
		if vc1.Get(k) != v {
			t.Errorf("expected %s=%d, got %d", k, v, vc1.Get(k))
		}
	}
}

func TestVectorClockMergeAndTick(t *testing.T) {
	// Имитация получения сообщения
	local := FromMap(map[string]uint64{"p1": 2, "p2": 1})
	received := FromMap(map[string]uint64{"p1": 1, "p2": 3})

	local.MergeAndTick(received, "p1")

	// p1 должен стать max(2, 1) + 1 = 3
	if local.Get("p1") != 3 {
		t.Errorf("expected p1=3, got %d", local.Get("p1"))
	}

	// p2 должен стать max(1, 3) = 3
	if local.Get("p2") != 3 {
		t.Errorf("expected p2=3, got %d", local.Get("p2"))
	}
}

func TestVectorClockSerialization(t *testing.T) {
	vc := FromMap(map[string]uint64{
		"process-1": 42,
		"process-2": 100,
		"process-3": 7,
	})

	data := vc.Marshal()
	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if !vc.IsEqual(restored) {
		t.Error("restored clock not equal to original")
	}
}

func TestLeastUpperBound(t *testing.T) {
	vc1 := FromMap(map[string]uint64{"p1": 3, "p2": 1})
	vc2 := FromMap(map[string]uint64{"p1": 1, "p2": 4})
	vc3 := FromMap(map[string]uint64{"p1": 2, "p2": 2, "p3": 5})

	lub := LeastUpperBound(vc1, vc2, vc3)

	expected := map[string]uint64{"p1": 3, "p2": 4, "p3": 5}
	for k, v := range expected {
		if lub.Get(k) != v {
			t.Errorf("expected %s=%d, got %d", k, v, lub.Get(k))
		}
	}
}

func TestConcurrentEvents(t *testing.T) {
	// Два независимых события без причинной связи
	vc1 := FromMap(map[string]uint64{"p1": 1})
	vc2 := FromMap(map[string]uint64{"p2": 1})

	if !vc1.IsConcurrent(vc2) {
		t.Error("events should be concurrent")
	}

	// Событие после merge уже не параллельно
	vc3 := vc1.Clone()
	vc3.MergeAndTick(vc2, "p1")

	if vc3.IsConcurrent(vc1) {
		t.Error("vc3 should happen after vc1")
	}
	if vc3.IsConcurrent(vc2) {
		t.Error("vc3 should happen after vc2")
	}
}

// Пример из пространственно-временной диаграммы лекции
func TestSpaceTimeDiagram(t *testing.T) {
	// 4 процесса: p1, p2, p3, p4
	// События: a, b, c, d, e, f, g, h, i, j, k, l

	// p1: a -> b -> f -> d
	// p2: g -> h -> j -> k
	// p3: e -> l
	// p4: c -> i
	// Сообщения: b->j, f->h, d->k, g->l, k->i

	events := map[string]*VectorClock{
		"a": FromMap(map[string]uint64{"p1": 1}),
		"b": FromMap(map[string]uint64{"p1": 2}),
		"g": FromMap(map[string]uint64{"p2": 1}),
		"h": FromMap(map[string]uint64{"p1": 2, "p2": 2}), // после f->h
		"f": FromMap(map[string]uint64{"p1": 3}),
		"j": FromMap(map[string]uint64{"p1": 2, "p2": 3}), // после b->j
	}

	// a ≺ b (один процесс)
	if !events["a"].HappensBefore(events["b"]) {
		t.Error("a should happen before b")
	}

	// b ≺ j (через сообщение)
	if !events["b"].HappensBefore(events["j"]) {
		t.Error("b should happen before j")
	}

	// a ∥ g (разные процессы, нет связи)
	if !events["a"].IsConcurrent(events["g"]) {
		t.Error("a and g should be concurrent")
	}
}
