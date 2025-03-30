package main

import (
	"fmt"
	"log"
	"strconv"

	"pl.parquet/db"
	"pl.parquet/db/model"
)

func main() {
	reader()
	// writer()
}

func reader() {
	db.NewParquetReader("people-table.parquet")
}

func writer() {
	size := 1000

	abc := "abcdefghijklmnoprstuwz"
	ages := []int32{23, 45, 56, 24, 43, 42}

	// set bufferSize to the number of concurrent logs you are going to write to avoid blocking
	l := db.NewLogger[model.MessageModel]("./data/", 10, 1)
	l.Start()
	defer l.Stop()
	data := make([][]string, size)
	abcIdx := 0
	for i := 0; i < size; i++ {
		value := "xxx"
		if abcIdx+3 > len(abc) {
			abcIdx = 0
		}
		age := ages[i%len(ages)]
		value = abc[abcIdx : abcIdx+3]
		abcIdx++
		mess := model.MessageModel{
			ID:    int64(i + 1),
			Value: value,
			Age:   age,
			Score: float64(age) * 2.33,
			Name:  "name_" + value,
		}
		l.AddLog(mess)
		data[i] = []string{strconv.Itoa(int(mess.ID)), mess.Value, fmt.Sprint(mess.Age), fmt.Sprint(mess.Score), mess.Name}
	}
	csvWriter, err := db.NewCsvWriter("./data/")
	if err != nil {
		log.Println("error when create csv writer", err)
		return
	}
	headers := []string{"ID", "Value", "Age", "Score", "Name"}
	csvWriter.Write(headers, data)
	csvWriter.Close()
}
