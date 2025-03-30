package db

import (
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"
)

type Logger[TLog any] interface {
	AddLog(TLog)
	Start() error
	Stop()
}

type logger[TLog any] struct {
	logDir           string
	buffer           chan TLog
	wg               sync.WaitGroup
	flustIntervalSec int
}

func NewLogger[TLog any](logDir string, bufferSize, flustIntervalSec int) Logger[TLog] {
	l := &logger[TLog]{
		logDir:           logDir,
		buffer:           make(chan TLog, bufferSize),
		flustIntervalSec: flustIntervalSec,
		wg:               sync.WaitGroup{},
	}
	return l
}

func (l *logger[TLog]) AddLog(r TLog) {
	l.buffer <- r
	// select {
	// case l.buffer <- r:
	// default:
	// 	// try to write to buffer and drop log instead of getting blocked in case buffer is full
	// 	log.Println("Dropping parquet log to avoid blocking")
	// }
}

func GenerateFileName(extension string) string {
	// create concerted file name
	return fmt.Sprintf("%s.%d.%s",
		time.Now().Format("2006-01-02-15-04-05"),
		time.Now().Nanosecond(),
		extension,
	)

}

func (l *logger[TLog]) Start() error {
	dirExists, err := exists(l.logDir)
	if err != nil {
		return err
	}
	if !dirExists {
		if e := os.MkdirAll(l.logDir, 0700); e != nil {
			return fmt.Errorf("creating log directory: %w", e)
		}
	}

	pl, err := newParquetLogger[TLog](path.Join(l.logDir, GenerateFileName("parquet")))
	if err != nil {
		return err
	}

	ticker := time.NewTicker(time.Duration(l.flustIntervalSec) * time.Second)

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		for {
			select {
			case record, ok := <-l.buffer:
				if ok {
					if e := pl.AddLog(record); e != nil {
						log.Fatalf("Failed to write parquet log:%v\n", e)
					}
				} else {
					// chan is closed, flush and return
					pl.Close()
					ticker.Stop()
					return
				}
			case <-ticker.C:
				// flush and rotate log
				if pl.HasLogs() {
					pl.Close()
					pl, err = newParquetLogger[TLog](path.Join(l.logDir, GenerateFileName("parquet")))
					if err != nil {
						log.Fatalf("Failed to create parquet log file: %v\n", err)
					}
				}
			}
		}
	}()

	return nil
}

func (l *logger[TLog]) Stop() {
	close(l.buffer)
	l.wg.Wait()
}

func exists(dir string) (bool, error) {
	_, err := os.Stat(dir)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, fmt.Errorf("getting dir stats: %w", err)
}
