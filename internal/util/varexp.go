package util

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"text/template"
)

var envRegExp = regexp.MustCompile(`\${env:.+?}`)
var msgRegExp = regexp.MustCompile(`\${msg:.+?}`)
var tmplActionRegExp = regexp.MustCompile(`{{(.*?)}}`)

func ExpandEnvVars(src string) string {
	return envRegExp.ReplaceAllStringFunc(src, func(s string) string {
		k, d, found := strings.Cut(fullVar(s), "|")
		if found {
			v, ok := os.LookupEnv(k)
			if ok {
				return v
			} else {
				return d
			}
		} else {
			return os.Getenv(k)
		}
	})
}

func HasMsgVar(s string) bool {
	return msgRegExp.MatchString(s)
}

func ConvertToMsgTmpl(src string) (*template.Template, error) {
	tmpl := tmplActionRegExp.ReplaceAllString(src, "{{\"{{\"}}$1{{\"}}\"}}")
	tmpl = msgRegExp.ReplaceAllStringFunc(tmpl, func(s string) string {
		fv := fullVar(s)
		if fv == "key" {
			return "{{.Key}}"
		} else if strings.HasPrefix(fv, "header.") && len(fv) > 7 {
			return fmt.Sprintf("{{.Headers.%s}}", fv[7:])
		}
		return ""
	})
	fmt.Println(tmpl)
	return template.New("msg").Parse(tmpl)
}

func fullVar(match string) string {
	return match[6 : len(match)-1]
}
