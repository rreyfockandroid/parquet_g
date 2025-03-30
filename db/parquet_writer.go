package db

import (
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"pl.parquet/db/model"
)

type parquetLogger[TLog any] struct {
	fw       source.ParquetFile
	pw       *writer.ParquetWriter
	filename string
}

const renamePattern = "%s.tmp"

func newParquetLogger[TLog any](filename string) (parquetLogger[TLog], error) {
	fw, err := local.NewLocalFileWriter(fmt.Sprintf(renamePattern, filename))
	if err != nil {
		return parquetLogger[TLog]{}, fmt.Errorf("creating local file writer: %w", err)
	}
	//parameters: writer, type of struct, size
	pw, err := writer.NewParquetWriter(fw, new(model.MessageModel), int64(runtime.NumCPU()))
	if err != nil {
		fw.Close()
		return parquetLogger[TLog]{}, fmt.Errorf("creating parquet writer: %w", err)
	}
	pw.RowGroupSize = 32 * 1024 * 1024 // 32M
	pw.PageSize = 8 * 1024             // 8K

	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	return parquetLogger[TLog]{fw: fw, pw: pw, filename: filename}, nil
}

func (p *parquetLogger[TLog]) AddLog(r TLog) error {
	return p.pw.Write(r)
}

func (p *parquetLogger[TLog]) HasLogs() bool {
	return len(p.pw.Objs) > 0
}

// nolint:ifshort
func (p *parquetLogger[TLog]) Close() {
	hadLogs := p.HasLogs()
	if err := p.pw.WriteStop(); err != nil {
		log.Printf("Error closing ParquetWriter: %v\n", err)
	}
	if err := p.fw.Close(); err != nil {
		log.Printf("Error closing ParquetFile: %v\n", err)
	}
	if hadLogs {
		// strip .tmp suffix
		if err := os.Rename(fmt.Sprintf(renamePattern, p.filename), p.filename); err != nil {
			log.Printf("Error renaming ParquetFile: %v\n", err)
		}
	} else {
		// remove empty log file
		if err := os.Remove(fmt.Sprintf(renamePattern, p.filename)); err != nil {
			log.Printf("Error removing empty ParquetFile: %v\n", err)
		}
	}
}
