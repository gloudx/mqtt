package permissions

// Visibility уровень видимости записи
type Visibility string

const (
	VisibilityPublic  Visibility = "public"  // Все члены семьи видят
	VisibilityPrivate Visibility = "private" // Только автор видит
	VisibilityShared  Visibility = "shared"  // Явно указанные участники видят
)
