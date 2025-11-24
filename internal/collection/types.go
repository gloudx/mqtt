package collection

import (
	"mqtt-http-tunnel/internal/eventlog"
	"mqtt-http-tunnel/internal/schema"
)

type Collection struct {
	name      string
	schema    *schema.SchemaDefinition
	storage   *Storage
	eventLog  *eventlog.EventLog // Ссылка на общий EventLog системы
	indexes   *IndexManager
	validator *Validator
}

type Document map[string]interface{}

// Operation представляет тип операции над документом
type Operation string

const (
	OpCreate       Operation = "create"
	OpUpdate       Operation = "update"
	OpDelete       Operation = "delete"
	OpCreateSchema Operation = "create_schema" // Создание новой схемы/коллекции
	OpUpdateSchema Operation = "update_schema" // Обновление схемы коллекции
)

// Query параметры запроса к коллекции
type Query struct {
	Filter map[string]interface{}
	Limit  int
	Offset int
}

// QueryResult результат запроса
type QueryResult struct {
	Documents []Document
	Total     int
}
