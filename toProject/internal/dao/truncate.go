package dao

import "strings"

func truncateUTF8(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) <= max {
		return s
	}

	cut := s[:max]
	if idx := strings.LastIndexFunc(cut, func(r rune) bool { return r == '\uFFFD' }); idx >= 0 {
		cut = cut[:idx]
	}

	for len(cut) > 0 && !isValidTail(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}

func isValidTail(s string) bool {
	if s == "" {
		return true
	}
	for i := len(s) - 1; i >= 0 && i > len(s)-4; i-- {
		b := s[i]
		if b < 0x80 {
			return true
		}
		if b&0xC0 == 0xC0 {
			need := 0
			switch {
			case b&0xE0 == 0xC0:
				need = 2
			case b&0xF0 == 0xE0:
				need = 3
			case b&0xF8 == 0xF0:
				need = 4
			default:
				return false
			}
			return len(s)-i == need
		}
	}
	return true
}
