package filter

import (
	"encoding/json"

	"github.com/sombr/go-datapump/core"
)

type toJsonFilter[T any] struct {
}

func (f *toJsonFilter[T]) Write(records []T) ([]string, error) {
	res := make([]string, 0)
	for _, rec := range records {
		buf, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}

		res = append(res, string(buf))
	}

	return res, nil
}

type fromJsonFilter[T any] struct {
}

func (f *fromJsonFilter[T]) Write(lines []string) ([]T, error) {
	var rec T
	records := make([]T, 0)

	for _, line := range lines {
		err := json.Unmarshal([]byte(line), &rec)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, nil
}

func NewToJSONFilter[T any]() core.Filter[T, string] {
	return &toJsonFilter[T]{}
}

func NewFromJSONFilter[T any]() core.Filter[string, T] {
	return &fromJsonFilter[T]{}
}
