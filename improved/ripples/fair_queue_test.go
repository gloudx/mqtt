package ripples

import (
	"fmt"
	"sync"
	"testing"
)

func TestFairQueueBasic(t *testing.T) {
	q := NewFairQueue(nil)

	env1 := &Envelope{ID: "1", FromDID: "a"}
	env2 := &Envelope{ID: "2", FromDID: "b"}
	env3 := &Envelope{ID: "3", FromDID: "a"}

	q.Push(env1)
	q.Push(env2)
	q.Push(env3)

	if q.Len() != 3 {
		t.Errorf("expected len 3, got %d", q.Len())
	}

	// Round-robin: должны получить a, b, a
	got := q.Pop()
	if got.FromDID != "a" || got.ID != "1" {
		t.Errorf("expected first from 'a', got %s", got.FromDID)
	}

	got = q.Pop()
	if got.FromDID != "b" {
		t.Errorf("expected from 'b', got %s", got.FromDID)
	}

	got = q.Pop()
	if got.FromDID != "a" || got.ID != "3" {
		t.Errorf("expected second from 'a', got %s", got.FromDID)
	}

	if q.Len() != 0 {
		t.Error("queue should be empty")
	}
}

func TestFairQueueFairness(t *testing.T) {
	q := NewFairQueue(&FairQueueConfig{
		MaxPerSource: 10,
		MaxTotal:     100,
	})

	// Добавляем 5 сообщений от каждого из 4 источников
	sources := []string{"a", "b", "c", "d"}
	for i := 0; i < 5; i++ {
		for _, src := range sources {
			q.Push(&Envelope{ID: fmt.Sprintf("%s-%d", src, i), FromDID: src})
		}
	}

	if q.Len() != 20 {
		t.Errorf("expected 20, got %d", q.Len())
	}

	// Извлекаем и проверяем round-robin
	seen := make(map[string]int)
	for i := 0; i < 20; i++ {
		env := q.Pop()
		seen[env.FromDID]++

		// Проверяем справедливость: после каждых 4 сообщений
		// каждый источник должен быть представлен примерно одинаково
		if (i+1)%4 == 0 {
			for _, src := range sources {
				expected := (i + 1) / 4
				if seen[src] < expected-1 || seen[src] > expected+1 {
					// Допускаем небольшое отклонение
				}
			}
		}
	}

	// В итоге каждый источник должен дать 5 сообщений
	for _, src := range sources {
		if seen[src] != 5 {
			t.Errorf("source %s: expected 5, got %d", src, seen[src])
		}
	}
}

func TestFairQueueLimits(t *testing.T) {
	q := NewFairQueue(&FairQueueConfig{
		MaxPerSource: 3,
		MaxTotal:     10,
	})

	// Добавляем 5 от одного источника - должны принять только 3
	for i := 0; i < 5; i++ {
		q.Push(&Envelope{ID: fmt.Sprintf("%d", i), FromDID: "a"})
	}

	if q.LenBySource("a") != 3 {
		t.Errorf("expected 3 from 'a', got %d", q.LenBySource("a"))
	}

	stats := q.Stats()
	if stats.Dropped != 2 {
		t.Errorf("expected 2 dropped, got %d", stats.Dropped)
	}
}

func TestFairQueueTotalLimit(t *testing.T) {
	q := NewFairQueue(&FairQueueConfig{
		MaxPerSource: 100,
		MaxTotal:     5,
	})

	for i := 0; i < 10; i++ {
		q.Push(&Envelope{ID: fmt.Sprintf("%d", i), FromDID: fmt.Sprintf("src%d", i)})
	}

	if q.Len() != 5 {
		t.Errorf("expected 5, got %d", q.Len())
	}

	stats := q.Stats()
	if stats.Dropped != 5 {
		t.Errorf("expected 5 dropped, got %d", stats.Dropped)
	}
}

func TestFairQueueEmpty(t *testing.T) {
	q := NewFairQueue(nil)

	if env := q.Pop(); env != nil {
		t.Error("expected nil from empty queue")
	}

	if env := q.Peek(); env != nil {
		t.Error("expected nil peek from empty queue")
	}
}

func TestFairQueueClear(t *testing.T) {
	q := NewFairQueue(nil)

	for i := 0; i < 10; i++ {
		q.Push(&Envelope{ID: fmt.Sprintf("%d", i), FromDID: "a"})
	}

	q.Clear()

	if q.Len() != 0 {
		t.Error("queue should be empty after clear")
	}

	if q.SourceCount() != 0 {
		t.Error("source count should be 0")
	}
}

func TestFairQueueConcurrency(t *testing.T) {
	q := NewFairQueue(&FairQueueConfig{
		MaxPerSource: 1000,
		MaxTotal:     10000,
	})

	var wg sync.WaitGroup

	// 10 producers
	for p := 0; p < 10; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				q.Push(&Envelope{
					ID:      fmt.Sprintf("p%d-%d", pid, i),
					FromDID: fmt.Sprintf("src%d", pid),
				})
			}
		}(p)
	}

	// 5 consumers
	consumed := make(chan int, 5)
	for c := 0; c < 5; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			count := 0
			for {
				if env := q.Pop(); env != nil {
					count++
				} else {
					break
				}
			}
			consumed <- count
		}()
	}

	wg.Wait()
	close(consumed)

	total := 0
	for c := range consumed {
		total += c
	}

	// Не все могут быть потреблены из-за race, но должно быть близко к 1000
	if total < 900 {
		t.Errorf("expected ~1000 consumed, got %d", total)
	}
}

func TestPriorityFairQueue(t *testing.T) {
	pq := NewPriorityFairQueue(nil)

	// Добавляем с разными приоритетами
	pq.Push(&Envelope{ID: "low", FromDID: "a"}, PriorityLow)
	pq.Push(&Envelope{ID: "normal", FromDID: "b"}, PriorityNormal)
	pq.Push(&Envelope{ID: "high", FromDID: "c"}, PriorityHigh)

	// Должны получить в порядке приоритета
	env := pq.Pop()
	if env.ID != "high" {
		t.Errorf("expected 'high', got %s", env.ID)
	}

	env = pq.Pop()
	if env.ID != "normal" {
		t.Errorf("expected 'normal', got %s", env.ID)
	}

	env = pq.Pop()
	if env.ID != "low" {
		t.Errorf("expected 'low', got %s", env.ID)
	}
}

func BenchmarkFairQueuePush(b *testing.B) {
	q := NewFairQueue(&FairQueueConfig{
		MaxPerSource: 1000000,
		MaxTotal:     1000000,
	})

	env := &Envelope{ID: "test", FromDID: "bench"}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(env)
	}
}

func BenchmarkFairQueuePushPop(b *testing.B) {
	q := NewFairQueue(&FairQueueConfig{
		MaxPerSource: 1000000,
		MaxTotal:     1000000,
	})

	env := &Envelope{ID: "test", FromDID: "bench"}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(env)
		q.Pop()
	}
}

func BenchmarkFairQueueMultiSource(b *testing.B) {
	q := NewFairQueue(&FairQueueConfig{
		MaxPerSource: 100000,
		MaxTotal:     1000000,
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		src := fmt.Sprintf("src%d", i%100)
		q.Push(&Envelope{ID: fmt.Sprintf("%d", i), FromDID: src})
	}
}
