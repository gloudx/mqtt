package evstore

import (
	"bytes"
	"sort"

	"mqtt-http-tunnel/improved/vclock"
)

// MergeStrategy определяет стратегию разрешения конкурентных записей
// Ключевое свойство: все ноды должны прийти к одному результату
// БЕЗ дополнительной координации (детерминизм)
type MergeStrategy int

const (
	// MergeStrategyLWW - Last-Writer-Wins по TID
	// Простая, но может терять данные
	MergeStrategyLWW MergeStrategy = iota

	// MergeStrategyLexical - лексикографически по CID
	// Полностью детерминистична, не зависит от времени
	MergeStrategyLexical

	// MergeStrategyVC - по Vector Clocks (если доступны)
	// Выбирает "наиболее продвинутый" по причинности
	MergeStrategyVC

	// MergeStrategyCustom - пользовательская функция
	MergeStrategyCustom
)

// MergeResolver определяет победителя среди конкурентных записей
type MergeResolver struct {
	strategy MergeStrategy
	custom   func([]*Record) *Record // Для MergeStrategyCustom
}

// NewMergeResolver создаёт resolver с заданной стратегией
func NewMergeResolver(strategy MergeStrategy) *MergeResolver {
	return &MergeResolver{strategy: strategy}
}

// NewCustomMergeResolver создаёт resolver с кастомной функцией
func NewCustomMergeResolver(fn func([]*Record) *Record) *MergeResolver {
	return &MergeResolver{
		strategy: MergeStrategyCustom,
		custom:   fn,
	}
}

// Resolve выбирает победителя среди конкурентных записей
// Гарантия: для одного и того же набора записей всегда вернёт один результат
func (mr *MergeResolver) Resolve(records []*Record) *Record {
	if len(records) == 0 {
		return nil
	}
	if len(records) == 1 {
		return records[0]
	}

	// Создаём копию для сортировки
	candidates := make([]*Record, len(records))
	copy(candidates, records)

	switch mr.strategy {
	case MergeStrategyLWW:
		return mr.resolveLWW(candidates)
	case MergeStrategyLexical:
		return mr.resolveLexical(candidates)
	case MergeStrategyVC:
		return mr.resolveVC(candidates)
	case MergeStrategyCustom:
		if mr.custom != nil {
			return mr.custom(candidates)
		}
		return mr.resolveLexical(candidates) // fallback
	default:
		return mr.resolveLexical(candidates)
	}
}

// resolveLWW - Last Writer Wins по TID
func (mr *MergeResolver) resolveLWW(records []*Record) *Record {
	sort.Slice(records, func(i, j int) bool {
		if records[i].TID != records[j].TID {
			return records[i].TID > records[j].TID // Больший TID = победитель
		}
		// При равных TID - по CID для детерминизма
		return records[i].CID().Compare(records[j].CID()) > 0
	})
	return records[0]
}

// resolveLexical - по лексикографическому порядку CID
func (mr *MergeResolver) resolveLexical(records []*Record) *Record {
	sort.Slice(records, func(i, j int) bool {
		// Больший CID = победитель (детерминистично)
		return records[i].CID().Compare(records[j].CID()) > 0
	})
	return records[0]
}

// resolveVC - по Vector Clocks
// Если есть не-параллельные записи, выбирает "наиболее позднюю"
// Для параллельных - fallback на lexical
func (mr *MergeResolver) resolveVC(records []*Record) *Record {
	// Фильтруем записи без VC
	withVC := make([]*Record, 0, len(records))
	for _, r := range records {
		if r.VC != nil {
			withVC = append(withVC, r)
		}
	}

	if len(withVC) == 0 {
		return mr.resolveLexical(records) // fallback
	}

	// Ищем запись, которая "после" всех остальных
	for _, candidate := range withVC {
		isLatest := true
		for _, other := range withVC {
			if candidate == other {
				continue
			}
			// Если candidate НЕ >= other, он не может быть победителем
			rel := candidate.VC.Compare(other.VC)
			if rel == vclock.Before {
				isLatest = false
				break
			}
		}
		if isLatest {
			return candidate
		}
	}

	// Все записи параллельны - fallback на lexical
	return mr.resolveLexical(records)
}

// ResolveCIDs выбирает победителя среди CID (для heads)
// Не требует загрузки полных записей
func ResolveCIDsLexical(cids []CID) CID {
	if len(cids) == 0 {
		return EmptyCID()
	}
	if len(cids) == 1 {
		return cids[0]
	}

	sorted := make([]CID, len(cids))
	copy(sorted, cids)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i][:], sorted[j][:]) > 0
	})

	return sorted[0]
}

// IsConcurrentSet проверяет, все ли записи параллельны друг другу
func IsConcurrentSet(records []*Record) bool {
	for i := 0; i < len(records); i++ {
		for j := i + 1; j < len(records); j++ {
			if !records[i].IsConcurrent(records[j]) {
				return false
			}
		}
	}
	return true
}

// FindCommonAncestors находит общих предков для набора записей
// Полезно для определения точки расхождения при форке
type AncestorFinder struct {
	getRecord func(CID) (*Record, error)
	cache     map[CID]*Record
}

func NewAncestorFinder(getRecord func(CID) (*Record, error)) *AncestorFinder {
	return &AncestorFinder{
		getRecord: getRecord,
		cache:     make(map[CID]*Record),
	}
}

// FindLCA находит Lowest Common Ancestor для двух записей
func (af *AncestorFinder) FindLCA(a, b CID) (CID, error) {
	if a.Equal(b) {
		return a, nil
	}

	// BFS от обеих записей
	visitedA := NewCIDSet()
	visitedB := NewCIDSet()

	queueA := []CID{a}
	queueB := []CID{b}

	for len(queueA) > 0 || len(queueB) > 0 {
		// Шаг от A
		if len(queueA) > 0 {
			current := queueA[0]
			queueA = queueA[1:]

			if visitedB.Has(current) {
				return current, nil // Нашли общего предка
			}

			visitedA.Add(current)

			record, err := af.get(current)
			if err != nil {
				continue
			}

			for _, parent := range record.Parents {
				if !visitedA.Has(parent) {
					queueA = append(queueA, parent)
				}
			}
		}

		// Шаг от B
		if len(queueB) > 0 {
			current := queueB[0]
			queueB = queueB[1:]

			if visitedA.Has(current) {
				return current, nil // Нашли общего предка
			}

			visitedB.Add(current)

			record, err := af.get(current)
			if err != nil {
				continue
			}

			for _, parent := range record.Parents {
				if !visitedB.Has(parent) {
					queueB = append(queueB, parent)
				}
			}
		}
	}

	return EmptyCID(), nil // Нет общего предка
}

func (af *AncestorFinder) get(cid CID) (*Record, error) {
	if r, ok := af.cache[cid]; ok {
		return r, nil
	}

	r, err := af.getRecord(cid)
	if err != nil {
		return nil, err
	}

	af.cache[cid] = r
	return r, nil
}

// ComputeDivergence вычисляет "расхождение" между ветками
// Возвращает записи, которые есть только в одной ветке
type Divergence struct {
	OnlyInA []CID
	OnlyInB []CID
	LCA     CID
}

func (af *AncestorFinder) ComputeDivergence(a, b CID) (*Divergence, error) {
	lca, err := af.FindLCA(a, b)
	if err != nil {
		return nil, err
	}

	div := &Divergence{LCA: lca}

	// Собираем все записи от A до LCA
	div.OnlyInA, err = af.collectUntil(a, lca)
	if err != nil {
		return nil, err
	}

	// Собираем все записи от B до LCA
	div.OnlyInB, err = af.collectUntil(b, lca)
	if err != nil {
		return nil, err
	}

	return div, nil
}

func (af *AncestorFinder) collectUntil(from, until CID) ([]CID, error) {
	var result []CID
	visited := NewCIDSet()
	queue := []CID{from}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.Equal(until) || visited.Has(current) {
			continue
		}

		visited.Add(current)
		result = append(result, current)

		record, err := af.get(current)
		if err != nil {
			continue
		}

		for _, parent := range record.Parents {
			queue = append(queue, parent)
		}
	}

	return result, nil
}
