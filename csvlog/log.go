package csvlog

import (
	"encoding/csv"
	"os"
)

const noLog bool = true

type CsvLogger struct {
	csvWriter *csv.Writer
	columns   []string
}

func NewCsvLogger(path string, columns []string) *CsvLogger {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}

	w := csv.NewWriter(f)
	w.Write(columns)

	return &CsvLogger{
		columns:   columns,
		csvWriter: w,
	}
}

func (l *CsvLogger) Flush() {
	l.csvWriter.Flush()
}

func (l *CsvLogger) Log(values []string) {
	if noLog {
		return
	}
	l.MustLog(values)
}

func (l *CsvLogger) MustLog(values []string) {
	if len(values) != len(l.columns) {
		l.Flush()
		panic("wrong number of values for csv logger")
	}

	err := l.csvWriter.Write(values)
	if err != nil {
		l.Flush()
		panic("could not write values to csv log")
	}
}
