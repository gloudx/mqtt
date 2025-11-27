// internal/collection/validators.go
package collection

import (
	"encoding/json"
	"fmt"
	"math"
	"mqtt-http-tunnel/internal/schema"
	"regexp"
	"time"
)

// Validator валидирует документы по схеме
type Validator struct {
	schema  *schema.SchemaDefinition
	storage *Storage // Для проверки уникальности
}

// NewValidator создаёт новый валидатор
func NewValidator(schemaDef *schema.SchemaDefinition, storage *Storage) *Validator {
	return &Validator{
		schema:  schemaDef,
		storage: storage,
	}
}

// ValidationMode режим валидации
type ValidationMode int

const (
	ModeInsert ValidationMode = iota
	ModeUpdate
)

// Validate валидирует документ при вставке
func (v *Validator) Validate(doc Document) error {
	return v.ValidateWithMode(doc, ModeInsert, nil)
}

// ValidateUpdate валидирует документ при обновлении
func (v *Validator) ValidateUpdate(doc Document, existing Document) error {
	return v.ValidateWithMode(doc, ModeUpdate, existing)
}

// ValidateWithMode валидирует документ с указанным режимом
func (v *Validator) ValidateWithMode(doc Document, mode ValidationMode, existing Document) error {
	for _, field := range v.schema.Fields {
		val, exists := doc[field.Name]

		// Проверка обязательных полей
		if !field.Nullable && !exists {
			// При update пропускаем отсутствующие поля (partial update)
			if mode == ModeUpdate {
				continue
			}
			return fmt.Errorf("field %s is required", field.Name)
		}

		// Проверка @immutable при обновлении
		if mode == ModeUpdate && exists && existing != nil {
			if v.hasDirective(field, "immutable") {
				oldVal, oldExists := existing[field.Name]
				if oldExists && oldVal != val {
					return fmt.Errorf("field %s is immutable and cannot be changed", field.Name)
				}
			}
		}

		if exists && val != nil {
			if err := v.validateField(field, val); err != nil {
				return err
			}

			// Проверка уникальности
			if v.hasDirective(field, "unique") && v.storage != nil {
				if err := v.validateUnique(field.Name, val, doc); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// ApplyDefaults применяет значения по умолчанию
func (v *Validator) ApplyDefaults(doc Document) Document {
	result := make(Document, len(doc))
	for k, val := range doc {
		result[k] = val
	}

	for _, field := range v.schema.Fields {
		if _, exists := result[field.Name]; exists {
			continue
		}

		for _, directive := range field.Directives {
			if directive.Name == "default" {
				if defaultVal, ok := directive.Arguments["value"]; ok {
					result[field.Name] = v.parseDefaultValue(defaultVal, field.Type)
				}
			}
		}
	}
	return result
}

func (v *Validator) parseDefaultValue(val interface{}, fieldType schema.FieldType) interface{} {
	strVal, ok := val.(string)
	if !ok {
		return val
	}

	switch fieldType {
	case schema.TypeInt:
		var i int
		fmt.Sscanf(strVal, "%d", &i)
		return i
	case schema.TypeFloat:
		var f float64
		fmt.Sscanf(strVal, "%f", &f)
		return f
	case schema.TypeBoolean:
		return strVal == "true"
	default:
		return strVal
	}
}

func (v *Validator) validateField(field schema.FieldDef, val interface{}) error {
	// Валидация списков
	if field.IsList {
		return v.validateList(field, val)
	}

	if err := v.validateType(field, val); err != nil {
		return err
	}

	for _, directive := range field.Directives {
		if err := v.validateDirective(field.Name, directive, val); err != nil {
			return err
		}
	}

	return nil
}

func (v *Validator) validateList(field schema.FieldDef, val interface{}) error {
	slice, ok := val.([]interface{})
	if !ok {
		return fmt.Errorf("field %s must be a list", field.Name)
	}

	// Создаём временное поле для валидации элементов
	elementField := schema.FieldDef{
		Name:       field.Name + "[]",
		Type:       field.Type,
		Nullable:   true, // Элементы списка могут быть nullable
		IsList:     false,
		Directives: field.Directives,
	}

	for i, item := range slice {
		if item == nil {
			continue
		}
		if err := v.validateType(elementField, item); err != nil {
			return fmt.Errorf("field %s[%d]: %w", field.Name, i, err)
		}
	}

	return nil
}

func (v *Validator) validateType(field schema.FieldDef, val interface{}) error {
	switch field.Type {
	case schema.TypeString, schema.TypeID:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("field %s must be string, got %T", field.Name, val)
		}

	case schema.TypeInt:
		switch num := val.(type) {
		case int, int32, int64:
			// OK
		case float64:
			// JSON парсит числа как float64, проверяем целочисленность
			if num != math.Trunc(num) {
				return fmt.Errorf("field %s must be integer, got float %v", field.Name, num)
			}
		default:
			return fmt.Errorf("field %s must be int, got %T", field.Name, val)
		}

	case schema.TypeFloat:
		switch val.(type) {
		case float32, float64, int, int32, int64:
			// OK - целые числа тоже допустимы для float
		default:
			return fmt.Errorf("field %s must be float, got %T", field.Name, val)
		}

	case schema.TypeBoolean:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("field %s must be boolean, got %T", field.Name, val)
		}

	case schema.TypeDateTime:
		strVal, ok := val.(string)
		if !ok {
			return fmt.Errorf("field %s must be datetime string, got %T", field.Name, val)
		}
		// Проверяем формат ISO 8601
		if _, err := time.Parse(time.RFC3339, strVal); err != nil {
			// Пробуем альтернативные форматы
			if _, err := time.Parse("2006-01-02", strVal); err != nil {
				return fmt.Errorf("field %s has invalid datetime format: %s", field.Name, strVal)
			}
		}

	case schema.TypeJSON:
		// JSON может быть любым типом: map, slice, string, number, bool, nil
		switch val.(type) {
		case map[string]interface{}, []interface{}, string, float64, bool, nil:
			// OK
		default:
			return fmt.Errorf("field %s must be valid JSON value, got %T", field.Name, val)
		}
	}

	return nil
}

func (v *Validator) validateDirective(fieldName string, directive schema.DirectiveConfig, val interface{}) error {
	switch directive.Name {
	case "validate":
		return v.validatePattern(fieldName, directive, val)
	case "encrypted":
		// Пропускаем - обрабатывается при сохранении
		return nil
	}
	return nil
}

func (v *Validator) validatePattern(fieldName string, directive schema.DirectiveConfig, val interface{}) error {
	// Проверка паттерна
	if pattern, ok := directive.Arguments["pattern"].(string); ok {
		strVal, ok := val.(string)
		if !ok {
			return nil // Паттерн применяется только к строкам
		}

		re, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("invalid pattern for field %s: %w", fieldName, err)
		}

		if !re.MatchString(strVal) {
			return fmt.Errorf("field %s does not match pattern %s", fieldName, pattern)
		}
	}

	// Проверка минимальной длины/значения
	if minVal, ok := directive.Arguments["min"]; ok {
		min := v.toInt(minVal)
		switch typedVal := val.(type) {
		case string:
			if len(typedVal) < min {
				return fmt.Errorf("field %s must be at least %d characters", fieldName, min)
			}
		case int:
			if typedVal < min {
				return fmt.Errorf("field %s must be at least %d", fieldName, min)
			}
		case float64:
			if typedVal < float64(min) {
				return fmt.Errorf("field %s must be at least %d", fieldName, min)
			}
		}
	}

	// Проверка максимальной длины/значения
	if maxVal, ok := directive.Arguments["max"]; ok {
		max := v.toInt(maxVal)
		switch typedVal := val.(type) {
		case string:
			if len(typedVal) > max {
				return fmt.Errorf("field %s must be at most %d characters", fieldName, max)
			}
		case int:
			if typedVal > max {
				return fmt.Errorf("field %s must be at most %d", fieldName, max)
			}
		case float64:
			if typedVal > float64(max) {
				return fmt.Errorf("field %s must be at most %d", fieldName, max)
			}
		}
	}

	return nil
}

func (v *Validator) validateUnique(fieldName string, val interface{}, currentDoc Document) error {
	if v.storage == nil {
		return nil
	}

	currentID, _ := currentDoc["id"].(string)

	// Сканируем все документы для проверки уникальности
	// TODO: Оптимизировать через индексы
	var found bool
	err := v.storage.Scan(v.storage.docPrefix(), func(key, value []byte) error {
		var doc Document
		if err := json.Unmarshal(value, &doc); err != nil {
			return nil // Пропускаем битые документы
		}

		// Пропускаем текущий документ при update
		if docID, ok := doc["id"].(string); ok && docID == currentID {
			return nil
		}

		if existingVal, exists := doc[fieldName]; exists && existingVal == val {
			found = true
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to check uniqueness: %w", err)
	}

	if found {
		return fmt.Errorf("field %s must be unique, value %v already exists", fieldName, val)
	}

	return nil
}

func (v *Validator) hasDirective(field schema.FieldDef, name string) bool {
	for _, d := range field.Directives {
		if d.Name == name {
			return true
		}
	}
	return false
}

func (v *Validator) toInt(val interface{}) int {
	switch n := val.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}
