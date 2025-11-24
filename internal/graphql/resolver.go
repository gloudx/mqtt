// internal/graphql/resolver.go
package graphql

import (
	"context"
	"encoding/base64"
	"fmt"
	"mqtt-http-tunnel/internal/collection"
	"mqtt-http-tunnel/internal/schema"
	"mqtt-http-tunnel/internal/tid"
	"strings"

	"github.com/graphql-go/graphql"
)

func NewResolver(engine *collection.Engine) *Resolver {
	return &Resolver{
		engine:        engine,
		filterBuilder: NewFilterBuilder(),
		schemaBuilder: NewSchemaBuilder(),
		inputTypes:    make(map[string]*graphql.InputObject),
		sortEnums:     make(map[string]*graphql.Enum),
	}
}

func (r *Resolver) Build() error {
	queryFields := graphql.Fields{}
	mutationFields := graphql.Fields{}

	collections := r.engine.ListCollections()

	for _, collectionName := range collections {
		col, err := r.engine.GetCollection(collectionName)
		if err != nil {
			continue
		}

		schemaDef := col.Schema()
		objType := r.schemaBuilder.BuildType(schemaDef)

		queryFields[collectionName] = &graphql.Field{
			Type: objType,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.ID),
				},
			},
			Resolve: r.makeGetResolver(collectionName),
		}

		filterInput := r.filterBuilder.BuildFilterInput(schemaDef)
		sortEnum := r.buildSortEnum(schemaDef)

		queryFields[collectionName+"s"] = &graphql.Field{
			Type: graphql.NewList(objType),
			Args: graphql.FieldConfigArgument{
				"limit": &graphql.ArgumentConfig{
					Type:         graphql.Int,
					DefaultValue: 10,
				},
				"offset": &graphql.ArgumentConfig{
					Type:         graphql.Int,
					DefaultValue: 0,
				},
				"filter": &graphql.ArgumentConfig{
					Type: filterInput,
				},
				"orderBy": &graphql.ArgumentConfig{
					Type: sortEnum,
				},
			},
			Resolve: r.makeListResolver(collectionName),
		}

		mutationFields["create"+collectionName] = &graphql.Field{
			Type: objType,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: r.buildInputType(schemaDef),
				},
			},
			Resolve: r.makeCreateResolver(collectionName),
		}

		mutationFields["update"+collectionName] = &graphql.Field{
			Type: objType,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.ID),
				},
				"input": &graphql.ArgumentConfig{
					Type: r.buildInputType(schemaDef),
				},
			},
			Resolve: r.makeUpdateResolver(collectionName),
		}

		mutationFields["delete"+collectionName] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(graphql.ID),
				},
			},
			Resolve: r.makeDeleteResolver(collectionName),
		}
	}

	// GraphQL требует хотя бы одно поле в Query
	if len(queryFields) == 0 {
		queryFields["_schema"] = &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return "No collections defined yet", nil
			},
			Description: "Placeholder field when no collections are defined",
		}
	}

	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name:   "Query",
		Fields: queryFields,
	})

	var mutationType *graphql.Object
	if len(mutationFields) > 0 {
		mutationType = graphql.NewObject(graphql.ObjectConfig{
			Name:   "Mutation",
			Fields: mutationFields,
		})
	}

	schemaConfig := graphql.SchemaConfig{
		Query:    queryType,
		Mutation: mutationType,
	}

	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		return err
	}

	r.schema = &schema
	return nil
}

func (r *Resolver) buildSortEnum(schemaDef *schema.SchemaDefinition) *graphql.Enum {
	// Проверяем кэш
	if enum, exists := r.sortEnums[schemaDef.Name]; exists {
		return enum
	}

	values := graphql.EnumValueConfigMap{}

	for _, field := range schemaDef.Fields {
		// Ascending
		values[strings.ToUpper(field.Name)+"_ASC"] = &graphql.EnumValueConfig{
			Value: field.Name + "_asc",
		}
		// Descending
		values[strings.ToUpper(field.Name)+"_DESC"] = &graphql.EnumValueConfig{
			Value: field.Name + "_desc",
		}
	}

	enum := graphql.NewEnum(graphql.EnumConfig{
		Name:   schemaDef.Name + "SortOrder",
		Values: values,
	})

	// Сохраняем в кэш
	r.sortEnums[schemaDef.Name] = enum
	return enum
}

func (r *Resolver) buildInputType(schemaDef *schema.SchemaDefinition) *graphql.InputObject {
	// Проверяем кэш
	if inputType, exists := r.inputTypes[schemaDef.Name]; exists {
		return inputType
	}

	fields := graphql.InputObjectConfigFieldMap{}

	for _, field := range schemaDef.Fields {
		if field.Name == "id" {
			continue
		}

		var baseType graphql.Input

		switch field.Type {
		case schema.TypeString:
			baseType = graphql.String
		case schema.TypeInt:
			baseType = graphql.Int
		case schema.TypeFloat:
			baseType = graphql.Float
		case schema.TypeBoolean:
			baseType = graphql.Boolean
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

		fields[field.Name] = &graphql.InputObjectFieldConfig{
			Type: baseType,
		}
	}

	inputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   schemaDef.Name + "Input",
		Fields: fields,
	})

	// Сохраняем в кэш
	r.inputTypes[schemaDef.Name] = inputType
	return inputType
}

func (r *Resolver) makeGetResolver(collectionName string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		id, ok := p.Args["id"].(string)
		if !ok {
			return nil, fmt.Errorf("invalid id")
		}

		col, err := r.engine.GetCollection(collectionName)
		if err != nil {
			return nil, err
		}

		return col.Get(p.Context, id)
	}
}

func (r *Resolver) makeListResolver(collectionName string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		col, err := r.engine.GetCollection(collectionName)
		if err != nil {
			return nil, fmt.Errorf("collection not found: %w", err)
		}

		query := collection.Query{
			Limit:  10,
			Offset: 0,
		}

		if limit, ok := p.Args["limit"].(int); ok {
			if limit > 100 {
				return nil, fmt.Errorf("limit cannot exceed 100")
			}
			if limit < 1 {
				return nil, fmt.Errorf("limit must be at least 1")
			}
			query.Limit = limit
		}

		if offset, ok := p.Args["offset"].(int); ok {
			if offset < 0 {
				return nil, fmt.Errorf("offset must be non-negative")
			}
			query.Offset = offset
		}

		// Apply filters
		if filters, ok := p.Args["filter"].(map[string]interface{}); ok && len(filters) > 0 {
			schemaDef := col.Schema()
			query.Filter = r.filterBuilder.ApplyFilters(filters, schemaDef)
		}

		// Note: Sorting will need to be implemented in the collection package
		// For now, orderBy parameter is accepted but not used
		// TODO: Add sorting support to collection.Query

		result, err := col.Query(p.Context, query)
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		return result.Documents, nil
	}
}

func (r *Resolver) makeCreateResolver(collectionName string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		input, ok := p.Args["input"].(map[string]interface{})
		if !ok || input == nil {
			return nil, fmt.Errorf("invalid input: expected object, got nil or wrong type")
		}

		col, err := r.engine.GetCollection(collectionName)
		if err != nil {
			return nil, fmt.Errorf("collection not found: %w", err)
		}

		// Generate TID if no ID provided
		if _, hasID := input["id"]; !hasID {
			input["id"] = string(tid.NewTIDNow(0))
		}

		_, err = col.Insert(p.Context, input)
		if err != nil {
			return nil, fmt.Errorf("insert failed: %w", err)
		}

		return input, nil
	}
}

func (r *Resolver) makeUpdateResolver(collectionName string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		id, ok := p.Args["id"].(string)
		if !ok || id == "" {
			return nil, fmt.Errorf("invalid id: expected non-empty string")
		}

		input, ok := p.Args["input"].(map[string]interface{})
		if !ok || input == nil {
			return nil, fmt.Errorf("invalid input: expected object, got nil or wrong type")
		}

		if len(input) == 0 {
			return nil, fmt.Errorf("invalid input: no fields to update")
		}

		col, err := r.engine.GetCollection(collectionName)
		if err != nil {
			return nil, fmt.Errorf("collection not found: %w", err)
		}

		_, err = col.Update(p.Context, id, input)
		if err != nil {
			return nil, fmt.Errorf("update failed: %w", err)
		}

		doc, err := col.Get(p.Context, id)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve updated document: %w", err)
		}

		return doc, nil
	}
}

func (r *Resolver) makeDeleteResolver(collectionName string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		id, ok := p.Args["id"].(string)
		if !ok || id == "" {
			return nil, fmt.Errorf("invalid id: expected non-empty string")
		}

		col, err := r.engine.GetCollection(collectionName)
		if err != nil {
			return nil, fmt.Errorf("collection not found: %w", err)
		}

		_, err = col.Delete(p.Context, id)
		if err != nil {
			return nil, fmt.Errorf("delete failed: %w", err)
		}

		return true, nil
	}
}

func (r *Resolver) Execute(ctx context.Context, query string, variables map[string]interface{}) *graphql.Result {
	params := graphql.Params{
		Schema:         *r.schema,
		RequestString:  query,
		VariableValues: variables,
		Context:        ctx,
	}

	return graphql.Do(params)
}

func (r *Resolver) Schema() *graphql.Schema {
	return r.schema
}

// encodeCursor создает курсор для пагинации
func encodeCursor(id string) string {
	return base64.StdEncoding.EncodeToString([]byte(id))
}

// decodeCursor декодирует курсор
func decodeCursor(cursor string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return "", fmt.Errorf("invalid cursor: %w", err)
	}
	return string(data), nil
}
