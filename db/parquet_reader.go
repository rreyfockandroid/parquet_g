package db

import (
	"log"
	"path"
	"runtime"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"pl.parquet/db/model"
)

type parquetReader struct {
}

func NewParquetReader(filename string) parquetReader {
	fr, err := local.NewLocalFileReader(path.Join("data", filename))
	if err != nil {
		log.Println("Can't open file", err)
		return parquetReader{}
	}
	pr, err := reader.NewParquetReader(fr, new(model.MessageModel), int64(runtime.NumCPU()))
	if err != nil {
		log.Println("Can't create parquet reader")
		return parquetReader{}
	}
	num := int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]model.MessageModel, 1)
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}
	pr.ReadStop()
	fr.Close()
	return parquetReader{}
}
