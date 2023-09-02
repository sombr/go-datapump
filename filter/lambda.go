package filter

import "github.com/sombr/go-datapump/core"

type lambdaFilter[T any, S any] struct {
	processor func(rec T) (S, error)
}

func (f *lambdaFilter[T, S]) Write(records []T) ([]S, error) {
	res := make([]S, 0, len(records))
	for _, r := range records {
		s, err := f.processor(r)
		if err != nil {
			return nil, err
		}
		res = append(res, s)
	}

	return res, nil
}

func Lambda[T any, S any](processor func(rec T) (S, error)) core.Filter[T, S] {
	return &lambdaFilter[T, S]{processor: processor}
}
