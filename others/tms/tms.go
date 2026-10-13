package tms

import (
	"strconv"
	"strings"
	"time"
)

func Format(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}
func FormatByMilli(t int64) string {
	return time.UnixMilli(t).Format("2006-01-02 15:04:05")
}

var parseLayouts = []string{
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.000",
	"2006/01/02 15:04:05",
	"2006-01-02",
}

func ParseTime(str string) int64 {
	str = strings.TrimSpace(str)
	if str == "" {
		return 0
	}

	if v, err := strconv.ParseInt(str, 10, 64); err == nil {
		if v > 1e12 {
			return v
		}
		if v > 1e9 {
			return v * 1000
		}
		return v
	}

	for _, layout := range parseLayouts {
		if parsed, err := time.ParseInLocation(layout, str, time.Local); err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

func DateRangeToUnix(begin, end int64) []int64 {
	start := time.Unix(begin, 0)
	last := time.Unix(end, 0)

	var days []int64
	for d := start; !d.After(last); d = d.AddDate(0, 0, 1) {
		days = append(days, d.Unix())
	}
	return days
}
