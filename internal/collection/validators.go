// internal/collection/validators.go - обработка директив при валидации
package collection

import (
	"fmt"
	"mqtt-http-tunnel/internal/schema"
	"regexp"
)

type Validator struct {
	schema *schema.SchemaDefinition
}

func NewValidator(schemaDef *schema.SchemaDefinition) *Validator {
	return &Validator{
		schema: schemaDef,
	}
}

func (v *Validator) Validate(doc Document) error {
	for _, field := range v.schema.Fields {
		val, exists := doc[field.Name]

		if !field.Nullable && !exists {
			return fmt.Errorf("field %s is required", field.Name)
		}

		if exists && val != nil {
			if err := v.validateField(field, val); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *Validator) validateField(field schema.FieldDef, val interface{}) error {
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

func (v *Validator) validateType(field schema.FieldDef, val interface{}) error {
	switch field.Type {
	case schema.TypeString, schema.TypeID:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("field %s must be string", field.Name)
		}
	case schema.TypeInt:
		switch val.(type) {
		case int, int64, float64:
		default:
			return fmt.Errorf("field %s must be int", field.Name)
		}
	case schema.TypeBoolean:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("field %s must be boolean", field.Name)
		}
	}
	return nil
}

func (v *Validator) validateDirective(fieldName string, directive schema.DirectiveConfig, val interface{}) error {
	switch directive.Name {
	case "validate":
		return v.validatePattern(fieldName, directive, val)
	case "unique":
		return nil
	case "immutable":
		return nil
	}
	return nil
}

func (v *Validator) validatePattern(fieldName string, directive schema.DirectiveConfig, val interface{}) error {
	if pattern, ok := directive.Arguments["pattern"].(string); ok {
		strVal, ok := val.(string)
		if !ok {
			return nil
		}

		re, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("invalid pattern for field %s", fieldName)
		}

		if !re.MatchString(strVal) {
			return fmt.Errorf("field %s does not match pattern %s", fieldName, pattern)
		}
	}

	if min, ok := directive.Arguments["min"].(int); ok {
		strVal, ok := val.(string)
		if ok && len(strVal) < min {
			return fmt.Errorf("field %s must be at least %d characters", fieldName, min)
		}
	}

	if max, ok := directive.Arguments["max"].(int); ok {
		strVal, ok := val.(string)
		if ok && len(strVal) > max {
			return fmt.Errorf("field %s must be at most %d characters", fieldName, max)
		}
	}

	return nil
}
