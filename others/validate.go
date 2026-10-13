package common

import (
	"regexp"
)

func VerifyMobile(mobile string) bool {
	if mobile == "" || len(mobile) != 11 {
		return false
	}

	regular := `^(13[0-9]|14[014-9]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\d{8}$`
	reg := regexp.MustCompile(regular)
	return reg.MatchString(mobile)
}

func VerifyEmailFormat(email string) bool {
	if email == "" {
		return false
	}

	pattern := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	reg := regexp.MustCompile(pattern)
	return reg.MatchString(email)
}
