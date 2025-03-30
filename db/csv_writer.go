package db

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"
)

type csvWriter struct {
	writer *csv.Writer
	file   *os.File
}

func NewCsvWriter(logDir string) (csvWriter, error) {
	file, err := os.Create(path.Join(logDir, GenerateFileName("csv")))
	if err != nil {
		return csvWriter{}, fmt.Errorf("can't create file %w", err)
	}
	return csvWriter{
		writer: csv.NewWriter(file),
		file:   file,
	}, nil
}

func (w csvWriter) Write(headers []string, data [][]string) {
	w.writer.Write(headers)
	for _, row := range data {
		w.writer.Write(row)
	}
}

func (w csvWriter) Close() error {
	w.writer.Flush()
	return w.file.Close()
}
