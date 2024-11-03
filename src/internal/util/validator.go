package util

import (
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
)

func NotBlankStrs(fl validator.FieldLevel) bool {
	field := fl.Field()

	switch field.Kind() {
	case reflect.String:
		return NotBlankStr(field.String())
	case reflect.Slice:
		if field.Len() == 0 {
			return false
		}
		for i := range field.Len() {
			val := field.Index(i)
			if !NotBlankStr(val.String()) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func NotBlankStr(str string) bool {
	return len(strings.Trim(strings.TrimSpace(str), "\x1c\x1d\x1e\x1f")) > 0
}
