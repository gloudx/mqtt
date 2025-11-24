// internal/graphql/schema_builder.go
package graphql

import (
	"encoding/json"
	"mqtt-http-tunnel/internal/schema"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

var JSONType = graphql.NewScalar(graphql.ScalarConfig{
	Name:        "JSON",
	Description: "The `JSON` scalar type represents JSON values as specified by ECMA-404",
	Serialize: func(value interface{}) interface{} {
		return value
	},
	ParseValue: func(value interface{}) interface{} {
		return value
	},
	ParseLiteral: func(valueAST ast.Value) interface{} {
		switch valueAST := valueAST.(type) {
		case *ast.StringValue:
			var result interface{}
			if err := json.Unmarshal([]byte(valueAST.Value), &result); err == nil {
				return result
			}
			return valueAST.Value
		case *ast.IntValue:
			var result interface{}
			if err := json.Unmarshal([]byte(valueAST.Value), &result); err == nil {
				return result
			}
			return valueAST.Value
		case *ast.ObjectValue:
			obj := make(map[string]interface{})
			for _, field := range valueAST.Fields {
				obj[field.Name.Value] = field.Value.GetValue()
			}
			return obj
		default:
			return nil
		}
	},
})

type SchemaBuilder struct {
	types map[string]*graphql.Object
}

func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		types: make(map[string]*graphql.Object),
	}
}

func (sb *SchemaBuilder) BuildType(schemaDef *schema.SchemaDefinition) *graphql.Object {
	if t, exists := sb.types[schemaDef.Name]; exists {
		return t
	}

	fields := graphql.Fields{}

	for _, field := range schemaDef.Fields {
		gqlType := sb.mapType(field)
		fields[field.Name] = &graphql.Field{
			Type: gqlType,
		}
	}

	objType := graphql.NewObject(graphql.ObjectConfig{
		Name:   schemaDef.Name,
		Fields: fields,
	})

	sb.types[schemaDef.Name] = objType
	return objType
}

func (sb *SchemaBuilder) mapType(field schema.FieldDef) graphql.Output {
	var baseType graphql.Type

	switch field.Type {
	case schema.TypeString:
		baseType = graphql.String
	case schema.TypeInt:
		baseType = graphql.Int
	case schema.TypeFloat:
		baseType = graphql.Float
	case schema.TypeBoolean:
		baseType = graphql.Boolean
	case schema.TypeID:
		baseType = graphql.ID
	case schema.TypeDateTime:
		baseType = graphql.String
	case schema.TypeJSON:
		baseType = JSONType
	default:
		baseType = graphql.String
	}

	if field.IsList {
		baseType = graphql.NewList(baseType)
	}

	if !field.Nullable {
		baseType = graphql.NewNonNull(baseType)
	}

	return baseType
}
