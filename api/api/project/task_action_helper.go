package project

import (
	"test.com/others/encrypts"
	"test.com/others/tms"
)

func decryptOrZero(code string, fallback int64) int64 {
	if v := encrypts.DecryptNoErr(code); v > 0 {
		return v
	}
	if fallback > 0 {
		return fallback
	}
	return 0
}

func parseTimeOrZero(raw string) int64 {
	if raw == "" {
		return 0
	}
	return tms.ParseTime(raw)
}
