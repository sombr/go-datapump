package storage

import "github.com/sombr/go-datapump/core"

type ArrayIn[T any] struct {
	pos  int
	data []T
}

type ArrayOut[T any] struct {
	data *[]T
}

func (in *ArrayIn[T]) Read(count int) ([]T, error) {
	end := in.pos + count
	if end > len(in.data) {
		end = len(in.data)
	}

	res := in.data[in.pos:end]
	in.pos = end
	return res, nil
}

func (in *ArrayIn[T]) Commit() error {
	return nil
}

func (out *ArrayOut[T]) Write(records []T) error {
	*out.data = append(*out.data, records...)
	return nil
}

func (out *ArrayOut[T]) Commit() error {
	return nil
}

func NewArrayIn[T any](array []T) core.In[T] {
	return &ArrayIn[T]{pos: 0, data: array}
}

func NewArrayOut[T any](array *[]T) core.Out[T] {
	return &ArrayOut[T]{data: array}
}
