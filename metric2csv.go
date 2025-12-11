package logparser

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

// Metric2CSV persists []Metric to a CSV file.
// Columns: Time, SourceType, Name, Value, CF
type Metric2CSV struct {
	// IncludeHeader controls whether to write the CSV header row.
	// When Append is true and the file already exists with non-zero size,
	// the header is suppressed regardless of this flag.
	IncludeHeader bool
	// Comma specifies the field delimiter (default ',').
	Comma rune
	// Append controls whether to append to the output file (vs overwrite).
	Append bool
}

func NewMetric2CSV() *Metric2CSV {
	return &Metric2CSV{
		IncludeHeader: true,
		Comma:         ',',
		Append:        false,
	}
}

// WriteFile writes metrics to the given path as CSV.
// It ensures consistent column order: Time,SourceType,Name,Value,CF.
func (w *Metric2CSV) WriteFile(metrics []Metric, path string) error {
	flag := os.O_CREATE | os.O_WRONLY
	if w.Append {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return fmt.Errorf("open csv output: %w", err)
	}
	defer f.Close()

	writeHeader := w.IncludeHeader
	if w.Append {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			writeHeader = false
		}
	}

	cw := csv.NewWriter(f)
	if w.Comma != 0 {
		cw.Comma = w.Comma
	}

	if writeHeader {
		if err := cw.Write([]string{"Time", "SourceType", "Name", "Value"}); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
	}
	for _, m := range metrics {
		ts := m.StartTime
		if ts.IsZero() {
			// Keep empty if zero
		}
		timeStr := ""
		if !ts.IsZero() {
			// format with micros
			timeStr = ts.Format("2006/01/02-15:04:05.000000")
		}
		row := []string{
			timeStr,
			string(m.SourceType),
			m.Name,
			strconv.FormatFloat(m.Value, 'g', -1, 64),
		}
		if err := cw.Write(row); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}


