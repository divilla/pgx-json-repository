package pgxexec

import (
	"regexp"
	"strings"
)

func Quote(value string) string {
	if strings.ContainsAny(value, "\"") {
		return value
	}

	match, _ := regexp.MatchString("[A-Z]+", value)
	if match {
		return "\"" + value + "\""
	}

	for _, v := range keywords {
		if v == value {
			return "\"" + value + "\""
		}
	}

	return value
}

func UnQuote(value string) string {
	if !strings.ContainsAny(value, "\"") {
		return value
	}

	return strings.Replace(value, "\"", "", -1)
}

func QuoteRelationName(value string) string {
	if strings.ContainsAny(value, "\"") {
		return value
	}

	names := strings.Split(value, ".")
	l := len(names)
	if l == 1 {
		return Quote(names[0])
	}
	if names[0] == "public" {
		return Quote(names[1])
	}

	return Quote(names[0]) + "." + Quote(names[1])
}
