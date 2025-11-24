// internal/graphql/handler.go
package graphql

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	maxRequestSize = 1 << 20 // 1MB
)

type Handler struct {
	resolver *Resolver
}

func NewHandler(resolver *Resolver) *Handler {
	return &Handler{
		resolver: resolver,
	}
}

type ErrorResponse struct {
	Errors []ErrorDetail `json:"errors"`
}

type ErrorDetail struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		h.writeError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Limit request body size
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)

	var req struct {
		Query         string                 `json:"query"`
		Variables     map[string]interface{} `json:"variables"`
		OperationName string                 `json:"operationName,omitempty"`
	}

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		if err == io.EOF {
			h.writeError(w, "Request body is empty", http.StatusBadRequest)
		} else {
			h.writeError(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		}
		return
	}

	if req.Query == "" {
		h.writeError(w, "Query is required", http.StatusBadRequest)
		return
	}

	result := h.resolver.Execute(r.Context(), req.Query, req.Variables)

	if err := json.NewEncoder(w).Encode(result); err != nil {
		// If encoding fails, we can't send a proper error response
		// as headers are already written
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func (h *Handler) writeError(w http.ResponseWriter, message string, statusCode int) {
	w.WriteHeader(statusCode)
	response := ErrorResponse{
		Errors: []ErrorDetail{
			{Message: message},
		},
	}
	json.NewEncoder(w).Encode(response)
}
