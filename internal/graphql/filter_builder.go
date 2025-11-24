// internal/graphql/filter_builder.go
package graphql

import (
	"mqtt-http-tunnel/internal/schema"
	"sync"

	"github.com/graphql-go/graphql"
)

type FilterBuilder struct {
	stringFilter   *graphql.InputObject
	numberFilter   *graphql.InputObject
	dateTimeFilter *graphql.InputObject
	filterInputs   map[string]*graphql.InputObject
	once           sync.Once
	mu             sync.RWMutex
}

func NewFilterBuilder() *FilterBuilder {
	return &FilterBuilder{
		filterInputs: make(map[string]*graphql.InputObject),
	}
}

func (fb *FilterBuilder) initFilterTypes() {
	fb.once.Do(func() {
		fb.stringFilter = fb.createStringFilterType()
		fb.numberFilter = fb.createNumberFilterType()
		fb.dateTimeFilter = fb.createDateTimeFilterType()
	})
}

func (fb *FilterBuilder) BuildFilterInput(schemaDef *schema.SchemaDefinition) *graphql.InputObject {
	// Проверяем кэш
	fb.mu.RLock()
	if filterInput, exists := fb.filterInputs[schemaDef.Name]; exists {
		fb.mu.RUnlock()
		return filterInput
	}
	fb.mu.RUnlock()

	fb.mu.Lock()
	defer fb.mu.Unlock()

	// Двойная проверка на случай гонки
	if filterInput, exists := fb.filterInputs[schemaDef.Name]; exists {
		return filterInput
	}

	fields := graphql.InputObjectConfigFieldMap{}

	for _, field := range schemaDef.Fields {
		filterType := fb.getFilterTypeForField(field)
		if filterType != nil {
			fields[field.Name] = &graphql.InputObjectFieldConfig{
				Type: filterType,
			}
		}
	}

	filterInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   schemaDef.Name + "Filter",
		Fields: fields,
	})

	// Сохраняем в кэш
	fb.filterInputs[schemaDef.Name] = filterInput
	return filterInput
}

func (fb *FilterBuilder) getFilterTypeForField(field schema.FieldDef) graphql.Input {
	fb.initFilterTypes()
	switch field.Type {
	case schema.TypeString, schema.TypeID:
		return fb.stringFilter
	case schema.TypeInt, schema.TypeFloat:
		return fb.numberFilter
	case schema.TypeBoolean:
		return graphql.Boolean
	case schema.TypeDateTime:
		return fb.dateTimeFilter
	default:
		return nil
	}
}

func (fb *FilterBuilder) createStringFilterType() *graphql.InputObject {
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "StringFilter",
		Fields: graphql.InputObjectConfigFieldMap{
			"equals": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
			"contains": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
			"startsWith": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
			"endsWith": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
			"in": &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.String),
			},
		},
	})
}

func (fb *FilterBuilder) createNumberFilterType() *graphql.InputObject {
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "NumberFilter",
		Fields: graphql.InputObjectConfigFieldMap{
			"equals": &graphql.InputObjectFieldConfig{
				Type: graphql.Float,
			},
			"gt": &graphql.InputObjectFieldConfig{
				Type: graphql.Float,
			},
			"gte": &graphql.InputObjectFieldConfig{
				Type: graphql.Float,
			},
			"lt": &graphql.InputObjectFieldConfig{
				Type: graphql.Float,
			},
			"lte": &graphql.InputObjectFieldConfig{
				Type: graphql.Float,
			},
			"in": &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.Float),
			},
		},
	})
}

func (fb *FilterBuilder) createDateTimeFilterType() *graphql.InputObject {
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "DateTimeFilter",
		Fields: graphql.InputObjectConfigFieldMap{
			"equals": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
			"gt": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
			"gte": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
			"lt": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
			"lte": &graphql.InputObjectFieldConfig{
				Type: graphql.String,
			},
		},
	})
}

// ApplyFilters применяет фильтры к запросу коллекции
func (fb *FilterBuilder) ApplyFilters(filters map[string]interface{}, schemaDef *schema.SchemaDefinition) map[string]interface{} {
	result := make(map[string]interface{})

	for fieldName, filterValue := range filters {
		if filterMap, ok := filterValue.(map[string]interface{}); ok {
			for op, val := range filterMap {
				result[fieldName+"."+op] = val
			}
		} else {
			result[fieldName] = filterValue
		}
	}

	return result
}
