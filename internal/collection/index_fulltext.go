// internal/collection/index_fulltext.go
package collection

import (
	"fmt"
	"strings"
)

func NewFullTextIndex(storage *IndexStorage, field string) *FullTextIndex {
	return &FullTextIndex{
		storage: storage,
		field:   field,
	}
}

func (idx *FullTextIndex) Add(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	text := fmt.Sprintf("%v", value)
	tokens := idx.tokenize(text)

	indexName := fmt.Sprintf("fulltext:%s", idx.field)
	for _, token := range tokens {
		if err := idx.storage.Put(indexName, token, docID); err != nil {
			return err
		}
	}

	return nil
}

func (idx *FullTextIndex) Remove(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	text := fmt.Sprintf("%v", value)
	tokens := idx.tokenize(text)

	indexName := fmt.Sprintf("fulltext:%s", idx.field)
	for _, token := range tokens {
		if err := idx.storage.Delete(indexName, token, docID); err != nil {
			return err
		}
	}

	return nil
}

func (idx *FullTextIndex) Search(filter interface{}) ([]string, error) {
	query := fmt.Sprintf("%v", filter)
	tokens := idx.tokenize(query)

	if len(tokens) == 0 {
		return []string{}, nil
	}

	indexName := fmt.Sprintf("fulltext:%s", idx.field)
	
	allResults := make(map[string]int)
	for _, token := range tokens {
		prefix := idx.storage.indexPrefix(indexName, token)
		docIDs, err := idx.storage.Scan(prefix)
		if err != nil {
			return nil, err
		}

		for _, docID := range docIDs {
			allResults[docID]++
		}
	}

	result := make([]string, 0, len(allResults))
	for docID := range allResults {
		result = append(result, docID)
	}

	return result, nil
}

func (idx *FullTextIndex) tokenize(text string) []string {
	text = strings.ToLower(text)
	words := strings.Fields(text)
	
	tokens := make([]string, 0, len(words))
	for _, word := range words {
		word = strings.Trim(word, ".,!?;:\"'")
		if len(word) > 2 {
			tokens = append(tokens, word)
		}
	}

	return tokens
}