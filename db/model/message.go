package model

type MessageModel struct {
	ID    int64   `parquet:"name=id, type=INT64"`
	Value string  `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age   int32   `parquet:"name=age, type=INT32"`
	Name  string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Score float64 `parquet:"name=score, type=DOUBLE"`
}
