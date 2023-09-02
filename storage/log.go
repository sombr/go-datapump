package storage

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sombr/go-datapump/core"
)

type CloseableIn[T any] interface {
	core.In[T]
	Close() error
}

type CloseableOut[T any] interface {
	core.Out[T]
	Close() error
}

type TextFile struct {
	filepath string
}

type TextFileIn struct {
	file     *os.File
	pos      *os.File
	position int64
}

type TextFileOut struct {
	file *os.File
}

func NewTextFile(path string) *TextFile {
	return &TextFile{
		filepath: path,
	}
}

func (tf *TextFile) In() (CloseableIn[string], error) {
	fpos, err := os.OpenFile(tf.filepath+".pos", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	var buffer [24]byte
	read, err := fpos.Read(buffer[:])

	if err != nil && err != io.EOF {
		return nil, err
	}

	var position int64 = 0
	if read > 0 {
		position, err = strconv.ParseInt(string(buffer[:read]), 10, 64)
		if err != nil {
			return nil, err
		}
	}

	fv, err := os.OpenFile(tf.filepath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	fv.Seek(position, io.SeekStart)

	return &TextFileIn{
		file:     fv,
		pos:      fpos,
		position: position,
	}, nil
}

func (tf *TextFile) Out() (CloseableOut[string], error) {
	f, err := os.OpenFile(tf.filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	return &TextFileOut{
		file: f,
	}, nil
}

func (o *TextFileOut) Write(records []string) error {
	_, err := o.file.WriteString(strings.Join(records, "\n") + "\n")
	return err
}

func (o *TextFileOut) Commit() error {
	return o.file.Sync()
}

func (o *TextFileOut) Close() error {
	return o.file.Close()
}

func (in *TextFileIn) Read(count int) ([]string, error) {
	// this is super inefficient, but it's simple enough
	// to illustrate the concept
	res := make([]string, 0)
	buf := make([]byte, 0)

	var sym [1]byte
	for len(res) < count {
		_, err := in.file.Read(sym[:])
		if err == io.EOF {
			if len(buf) > 0 {
				// this should not normally happen
				// we should have consistent lines
				time.Sleep(1 * time.Second)
			}
			if len(res) == 0 {
				return nil, nil
			}
			return res, nil
		}

		if err != nil {
			return nil, err
		}

		if sym[0] == '\n' {
			res = append(res, string(buf))
			buf = buf[0:0]
		}

		buf = append(buf, sym[0])
	}

	return res, nil
}

func (in *TextFileIn) Commit() error {
	position, err := in.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	_, err = in.pos.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = in.pos.Write([]byte(fmt.Sprintf("%d", position)))
	if err != nil {
		return err
	}
	err = in.pos.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (in *TextFileIn) Close() error {
	posErr := in.pos.Close()
	fileErr := in.file.Close()
	if posErr != nil {
		return posErr
	}
	if fileErr != nil {
		return fileErr
	}
	return nil
}
