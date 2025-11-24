// internal/graphql/types.go
package graphql

import (
	"mqtt-http-tunnel/internal/collection"

	"github.com/graphql-go/graphql"
)

type Resolver struct {
	engine        *collection.Engine
	schema        *graphql.Schema
	filterBuilder *FilterBuilder
	schemaBuilder *SchemaBuilder
	inputTypes    map[string]*graphql.InputObject
	sortEnums     map[string]*graphql.Enum
}

type ResolverContext struct {
	CollectionName string
	Operation      string
}

type PageInfo struct {
	HasNextPage     bool   `json:"hasNextPage"`
	HasPreviousPage bool   `json:"hasPreviousPage"`
	StartCursor     string `json:"startCursor"`
	EndCursor       string `json:"endCursor"`
}

type Connection struct {
	Edges      []Edge   `json:"edges"`
	PageInfo   PageInfo `json:"pageInfo"`
	TotalCount int      `json:"totalCount"`
}

type Edge struct {
	Node   interface{} `json:"node"`
	Cursor string      `json:"cursor"`
}
