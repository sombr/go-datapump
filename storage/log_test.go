package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/sombr/go-datapump/core"
	"github.com/sombr/go-datapump/filter"
)

func TestRead(t *testing.T) {
	log := NewTextFile("../testdata/data.txt")
	if log == nil {
		t.Fatal("Init failure")
	}

	in, err := log.In()
	if err != nil {
		t.Fatal("IN Init failure: ", err)
	}

	lines, err := in.Read(100)
	if err != nil {
		t.Fatal("IN Read failure")
	}

	if len(lines) != 5 {
		t.Fatal("IN expected 5 lines, got: ", lines)
	}
}

func TestPartialRead(t *testing.T) {
	os.Remove("../testdata/data.txt.pos")

	log := NewTextFile("../testdata/data.txt")
	in, _ := log.In()
	lines, _ := in.Read(1)
	if lines[0] != "AAA" {
		t.Fatal("expected AAA")
	}

	lines, _ = in.Read(1)
	if lines[0] != "BBBBBB" {
		t.Fatal("expected BBBBBB")
	}

	in.Commit()

	in.Close()

	in, _ = log.In()
	lines, _ = in.Read(1)
	if lines[0] != "CCC" {
		t.Fatal("expected CCC")
	}

	lines, _ = in.Read(1)
	if lines[0] != "DDDDDD" {
		t.Fatal("expected DDDDDD")
	}
}

type SerializableType struct {
	Name   string
	Number int64
}

func TestWriteAndRead(t *testing.T) {
	os.Remove("../testdata/data.json.log")
	os.Remove("../testdata/data.json.log.pos")
	tojson := filter.NewToJSONFilter[SerializableType]()
	fromjson := filter.NewFromJSONFilter[SerializableType]()

	log := NewTextFile("../testdata/data.json.log")
	out, _ := log.Out()
	defer out.Close()

	fout := core.FilterOut[SerializableType, string](out, tojson)
	for idx := 0; idx < 10; idx += 1 {
		fout.Write([]SerializableType{
			SerializableType{Name: fmt.Sprintf("name: %d", idx), Number: int64(idx)},
		})
	}
	fout.Commit()

	in, _ := log.In()
	defer in.Close()

	fin := core.FilterIn[string, SerializableType](in, fromjson)
	read, _ := fin.Read(100)

	if read == nil || len(read) < 10 {
		t.Fatal("expected 10 items")
	}
	for idx := 0; idx < 10; idx += 1 {
		if read[idx].Number != int64(idx) {
			t.Fatal("incorrect item")
		}
	}
}

func TestPumper(t *testing.T) {
	os.Remove("../testdata/data.json.log")
	os.Remove("../testdata/data.json.log.pos")
	os.Remove("../testdata/testjson.log.pos")
	tojson := filter.NewToJSONFilter[SerializableType]()
	fromjson := filter.NewFromJSONFilter[SerializableType]()

	from, _ := NewTextFile("../testdata/testjson.log").In()
	defer from.Close()
	in := core.FilterIn[string, SerializableType](from, fromjson)

	to, _ := NewTextFile("../testdata/data.json.log").Out()
	defer to.Close()
	out := core.FilterOut[SerializableType, string](to, tojson)
	out = core.FilterOut[SerializableType, SerializableType](out, filter.Lambda(func(from SerializableType) (SerializableType, error) {
		from.Number = from.Number * 10
		return from, nil
	}))

	pumper := core.NewPumper[SerializableType](in, out, 2, 2)
	err := pumper.Pump()
	if err != nil {
		t.Fatal(err)
	}
}
