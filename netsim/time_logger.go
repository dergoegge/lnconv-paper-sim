package netsim

import "git.tu-berlin.de/dergoegge/bachelor_thesis/csvlog"

// A wrapper around csv logger that only logs once every interval period.
type TimestampLogger struct {
	csvLogger      *csvlog.CsvLogger
	Interval       uint64
	LastTimeLogged uint64
}

func (l *TimestampLogger) Flush() {
	l.csvLogger.Flush()
}

func (l *TimestampLogger) Log(time uint64, values []string) {
	if time-uint64(l.LastTimeLogged) <= l.Interval {
		return
	}

	l.LastTimeLogged = time

	l.csvLogger.Log(values)
}
