package core

type In[T any] interface {
	Read(count int) ([]T, error)
	Commit() error
}

type Out[T any] interface {
	Write(records []T) error
	Commit() error
}

type Filter[T any, S any] interface {
	Write(records []T) ([]S, error)
}

type FilteredIn[T any, S any] struct {
	in     In[T]
	filter Filter[T, S]
}

type FilteredOut[T any, S any] struct {
	out    Out[S]
	filter Filter[T, S]
}

type Pumper[T any] struct {
	in          In[T]
	out         Out[T]
	batchSize   int
	commitCount int
}

func FilterIn[T any, S any](in In[T], filter Filter[T, S]) In[S] {
	return &FilteredIn[T, S]{
		in:     in,
		filter: filter,
	}
}

func FilterOut[T any, S any](out Out[S], filter Filter[T, S]) Out[T] {
	return &FilteredOut[T, S]{
		out:    out,
		filter: filter,
	}
}

func (fin *FilteredIn[T, S]) Read(count int) ([]S, error) {
	batch, err := fin.in.Read(count)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, nil
	}

	return fin.filter.Write(batch)
}

func (fin *FilteredIn[T, S]) Commit() error {
	return fin.in.Commit()
}

func (fout *FilteredOut[T, S]) Write(records []T) error {
	batch, err := fout.filter.Write(records)
	if err != nil {
		return err
	}
	if batch == nil {
		return nil
	}
	return fout.out.Write(batch)
}

func (fout *FilteredOut[T, S]) Commit() error {
	return fout.out.Commit()
}

func NewPumper[T any](in In[T], out Out[T], batchSize int, commitCount int) *Pumper[T] {
	return &Pumper[T]{
		in:          in,
		out:         out,
		batchSize:   batchSize,
		commitCount: commitCount,
	}
}

func (p *Pumper[T]) Pump() error {
	processed := 0
	for {
		batch, err := p.in.Read(p.batchSize)
		if err != nil {
			return err
		}

		if batch == nil { // end
			return nil
		}

		err = p.out.Write(batch)
		if err != nil {
			return err
		}

		processed += len(batch)
		if processed > p.commitCount {
			processed = processed % p.commitCount
			err = p.out.Commit()
			if err != nil {
				return err
			}
			err = p.in.Commit()
			if err != nil {
				return err
			}
		}
	}

	err := p.out.Commit()
	if err != nil {
		return err
	}
	err = p.in.Commit()
	if err != nil {
		return err
	}

	return nil
}
