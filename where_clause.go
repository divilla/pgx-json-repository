package pgxexec

import (
	"github.com/pkg/errors"
	"strings"
)

type whereClause struct {
	statement     string
	statementArgs []interface{}
	valuesPlan    []FieldValue
	filterPlan    []FieldValue
	params        *params
}

func (c *whereClause) build() (string, error) {
	if c.statement != "" {
		var argsLen = len(c.statementArgs)
		var input = c.statement
		var output string
		var index = strings.Index(input, "?")
		var i = 0
		for index > 0 {
			if i < argsLen {
				output += input[:index] + c.params.get(c.statementArgs[i])
			}
			input = input[index+1:]
			index = strings.Index(input, "?")
			i++
		}
		if i > argsLen {
			return "", errors.Errorf("Question mark count (%v) does not match args count (%v).", i, argsLen)
		}

		return " WHERE " + output, nil
	}

	var exprs []string

	for _, v := range c.valuesPlan {
		if v.Value == nil {
			exprs = append(exprs, Quote(v.ColumnName)+" IS NULL")
		} else if v.StartsWith {
			exprs = append(exprs, Quote(v.ColumnName)+" ILIKE "+c.params.getStartsWith(v.Value))
		} else if v.EndsWith {
			exprs = append(exprs, Quote(v.ColumnName)+" ILIKE "+c.params.getEndsWith(v.Value))
		} else if v.Contains {
			exprs = append(exprs, Quote(v.ColumnName)+" ILIKE "+c.params.getContains(v.Value))
		} else {
			exprs = append(exprs, Quote(v.ColumnName)+" = "+c.params.get(v.Value))
		}
	}
	if len(exprs) > 0 {
		return " WHERE " + strings.Join(exprs, " AND "), nil
	}

	for _, v := range c.filterPlan {
		if v.Value != nil {
			if v.StartsWith {
				exprs = append(exprs, Quote(v.ColumnName)+" ILIKE "+c.params.getStartsWith(v.Value))
			} else if v.EndsWith {
				exprs = append(exprs, Quote(v.ColumnName)+" ILIKE "+c.params.getEndsWith(v.Value))
			} else if v.Contains {
				exprs = append(exprs, Quote(v.ColumnName)+" ILIKE "+c.params.getContains(v.Value))
			} else {
				exprs = append(exprs, Quote(v.ColumnName)+" = "+c.params.get(v.Value))
			}
		}
	}
	if len(exprs) > 0 {
		return " WHERE " + strings.Join(exprs, " AND "), nil
	}

	return "", nil
}
