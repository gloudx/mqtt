// internal/schema/parser.go - обновить парсинг
package schema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(gqlSchema string) (*SchemaDefinition, error) {

	schemaWithDirectives := p.injectDirectiveDefinitions(gqlSchema)

	source := &ast.Source{
		Input: schemaWithDirectives,
	}

	doc, err := gqlparser.LoadSchema(source)
	if err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}

	if len(doc.Types) == 0 {
		return nil, fmt.Errorf("no types found in schema")
	}

	var mainType *ast.Definition
	for _, t := range doc.Types {
		if !isBuiltinType(t.Name) && !isDirectiveType(t.Name) {
			mainType = t
			break
		}
	}

	if mainType == nil {
		return nil, fmt.Errorf("no user-defined type found")
	}

	def := &SchemaDefinition{
		Name:             mainType.Name,
		Fields:           []FieldDef{},
		Indexes:          []IndexDef{},
		SchemaDirectives: p.parseDirectives(mainType.Directives),
		//
		GraphQLSchema: gqlSchema,
	}

	for _, field := range mainType.Fields {
		fieldDef, indexDef := p.parseField(field)
		def.Fields = append(def.Fields, fieldDef)

		if indexDef != nil {
			def.Indexes = append(def.Indexes, *indexDef)
		}
	}

	// pretty print for debugging
	b, _ := json.MarshalIndent(def, "", "  ")
	fmt.Println(string(b))

	return def, nil
}

func (p *Parser) injectDirectiveDefinitions(schema string) string {
	directives := `
scalar DateTime

directive @index(type: String = "EXACT") on FIELD_DEFINITION
directive @crdt(type: String!) on FIELD_DEFINITION
directive @unique on FIELD_DEFINITION
directive @default(value: String!) on FIELD_DEFINITION
directive @validate(pattern: String, min: Int, max: Int) on FIELD_DEFINITION
directive @relation(field: String!) on FIELD_DEFINITION
directive @computed(expression: String!) on FIELD_DEFINITION
directive @ttl(seconds: Int!) on FIELD_DEFINITION
directive @encrypted on FIELD_DEFINITION
directive @immutable on FIELD_DEFINITION
directive @permission(roles: [String!]!) on FIELD_DEFINITION | OBJECT

`
	return directives + "\n" + schema
}

func (p *Parser) parseField(field *ast.FieldDefinition) (FieldDef, *IndexDef) {

	fieldDef := FieldDef{
		Name:       field.Name,
		Nullable:   true,
		Directives: p.parseDirectives(field.Directives),
	}

	fieldType := field.Type

	if fieldType.NonNull {
		fieldDef.Nullable = false
		if fieldType.Elem != nil {
			fieldType = fieldType.Elem
		}
	}

	if fieldType != nil && fieldType.Elem != nil {
		fieldDef.IsList = true
		fieldType = fieldType.Elem

		if fieldType != nil && fieldType.NonNull {
			if fieldType.Elem != nil {
				fieldType = fieldType.Elem
			}
		}
	}

	if fieldType != nil {
		fieldDef.Type = p.mapType(fieldType.NamedType)
	}

	var indexDef *IndexDef

	for _, directive := range field.Directives {
		switch directive.Name {
		case "index":
			indexDef = &IndexDef{
				Field: field.Name,
				Type:  IndexExact,
			}
			if arg := p.getDirectiveArg(directive, "type"); arg != nil {
				indexType := strings.ToUpper(p.argValueToString(arg))
				switch indexType {
				case "PREFIX":
					indexDef.Type = IndexPrefix
				case "RANGE":
					indexDef.Type = IndexRange
				case "FULLTEXT":
					indexDef.Type = IndexFullText
				case "EXACT":
					indexDef.Type = IndexExact
				}
			}

		case "crdt":
			if arg := p.getDirectiveArg(directive, "type"); arg != nil {
				crdtType := p.argValueToString(arg)
				if err := p.validateCRDTType(crdtType); err == nil {
					fieldDef.CRDTType = crdtType
				}
			}

		case "validate":
			// Validate directive arguments
			if err := p.validateDirectiveArgs(directive); err != nil {
				// Silently skip invalid directives
				continue
			}
		}
	}

	return fieldDef, indexDef
}

func (p *Parser) parseDirectives(directives ast.DirectiveList) []DirectiveConfig {
	result := make([]DirectiveConfig, 0, len(directives))

	for _, directive := range directives {
		config := DirectiveConfig{
			Name:      directive.Name,
			Arguments: make(map[string]any),
		}

		for _, arg := range directive.Arguments {
			config.Arguments[arg.Name] = p.argValueToInterface(arg.Value)
		}

		result = append(result, config)
	}

	return result
}

func (p *Parser) getDirectiveArg(directive *ast.Directive, name string) *ast.Argument {
	for _, arg := range directive.Arguments {
		if arg.Name == name {
			return arg
		}
	}
	return nil
}

func (p *Parser) argValueToString(arg *ast.Argument) string {
	return strings.Trim(arg.Value.Raw, "\"")
}

func (p *Parser) argValueToInterface(value *ast.Value) interface{} {

	switch value.Kind {

	case ast.IntValue:
		var i int
		fmt.Sscanf(value.Raw, "%d", &i)
		return i

	case ast.FloatValue:
		var f float64
		fmt.Sscanf(value.Raw, "%f", &f)
		return f

	case ast.StringValue:
		return strings.Trim(value.Raw, "\"")

	case ast.BooleanValue:
		return value.Raw == "true"

	case ast.ListValue:
		list := make([]interface{}, len(value.Children))
		for i, child := range value.Children {
			list[i] = p.argValueToInterface(child.Value)
		}
		return list

	case ast.ObjectValue:
		obj := make(map[string]interface{})
		for _, child := range value.Children {
			obj[child.Name] = p.argValueToInterface(child.Value)
		}
		return obj

	default:
		return value.Raw
	}
}

func (p *Parser) mapType(typeName string) FieldType {
	switch typeName {
	case "String":
		return TypeString
	case "Int":
		return TypeInt
	case "Float":
		return TypeFloat
	case "Boolean":
		return TypeBoolean
	case "ID":
		return TypeID
	case "DateTime":
		return TypeDateTime
	case "JSON":
		return TypeJSON
	default:
		return TypeString
	}
}

func isBuiltinType(name string) bool {
	builtins := []string{
		"Int", "Float", "String", "Boolean", "ID", "DateTime",
		"__Schema", "__Type", "__Field", "__InputValue",
		"__EnumValue", "__Directive", "__TypeKind", "__DirectiveLocation",
	}

	for _, builtin := range builtins {
		if name == builtin {
			return true
		}
	}

	return false
}

func isDirectiveType(name string) bool {
	return strings.HasPrefix(name, "__")
}

// validateCRDTType validates that the CRDT type is supported
func (p *Parser) validateCRDTType(crdtType string) error {
	validTypes := []string{"LWW", "COUNTER", "GSET", "ORSET", "MVREGISTER"}
	for _, valid := range validTypes {
		if strings.EqualFold(crdtType, valid) {
			return nil
		}
	}
	return fmt.Errorf("invalid CRDT type: %s", crdtType)
}

// validateDirectiveArgs validates directive arguments
func (p *Parser) validateDirectiveArgs(directive *ast.Directive) error {
	switch directive.Name {
	case "validate":
		// Check that at least one validation argument is provided
		if len(directive.Arguments) == 0 {
			return fmt.Errorf("@validate directive requires at least one argument")
		}
		// Validate min/max if present
		if minArg := p.getDirectiveArg(directive, "min"); minArg != nil {
			if maxArg := p.getDirectiveArg(directive, "max"); maxArg != nil {
				// Both present, validate min < max
				var min, max int
				fmt.Sscanf(minArg.Value.Raw, "%d", &min)
				fmt.Sscanf(maxArg.Value.Raw, "%d", &max)
				if min >= max {
					return fmt.Errorf("@validate: min (%d) must be less than max (%d)", min, max)
				}
			}
		}
	case "ttl":
		if arg := p.getDirectiveArg(directive, "seconds"); arg != nil {
			var seconds int
			fmt.Sscanf(arg.Value.Raw, "%d", &seconds)
			if seconds <= 0 {
				return fmt.Errorf("@ttl: seconds must be positive")
			}
		} else {
			return fmt.Errorf("@ttl directive requires 'seconds' argument")
		}
	case "permission":
		if arg := p.getDirectiveArg(directive, "roles"); arg == nil {
			return fmt.Errorf("@permission directive requires 'roles' argument")
		}
	}
	return nil
}
