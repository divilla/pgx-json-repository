package pgxexec

import (
	"strings"
)

type selectClause struct {
	distinct bool
	ptrStr   interface{}
	colPtrs  []FieldValue
}

func (c *selectClause) build() (string, error) {
	var q = ""

	if c.distinct {
		q += " DISTINCT"
	}

	if len(c.colPtrs) == 0 {
		return q + " *", nil
	}

	var cols []string
	for _, v := range c.colPtrs {
		if v.As != "" && v.As != v.ColumnName {
			cols = append(cols, Quote(v.ColumnName)+" AS "+Quote(v.As))
		}
		if v.JsonName != "" && v.JsonName != v.ColumnName {
			cols = append(cols, Quote(v.ColumnName)+" AS "+Quote(v.JsonName))
		} else {
			cols = append(cols, Quote(v.ColumnName))
		}
	}

	return q + " " + strings.Join(cols, ", "), nil
}
