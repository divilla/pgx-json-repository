package pgxjrep

import (
	"github.com/sirupsen/logrus"
	"strings"
)

type whereClause struct {
	schema        *DbSchema
	target        string
	colData       []ColumnData
	statement     string
	statementArgs []interface{}
	values        map[string]interface{}
	filter        map[string]interface{}
	params        *params
}

func (c *whereClause) build() string {
	var exprs []string

	if len(c.colData) > 0 {
		for _, v := range c.colData {
			if v.Value == nil {
				exprs = append(exprs, c.schema.Quote(v.DbName)+" IS NULL")
			} else if v.IsString {
				exprs = append(exprs, c.schema.Quote(v.DbName)+" LIKE "+c.params.get(v.Value))
			} else {
				exprs = append(exprs, c.schema.Quote(v.DbName)+" = "+c.params.get(v.Value))
			}
		}

		return " WHERE " + strings.Join(exprs, " AND ")
	}

	// build statement
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
			logrus.Panicf("Question mark count (%v) does not match args count (%v).", i, argsLen)
		}

		return " WHERE " + output
	}

	// build vals
	if len(c.values) > 0 {
		for _, v := range c.schema.ResolveColumnMap(c.target, c.values) {
			if v.Value == nil {
				exprs = append(exprs, c.schema.Quote(v.DbName)+" IS NULL")
			} else if v.IsString {
				exprs = append(exprs, c.schema.Quote(v.DbName)+" LIKE "+c.params.get(v.Value))
			} else {
				exprs = append(exprs, c.schema.Quote(v.DbName)+" = "+c.params.get(v.Value))
			}
		}

		return " WHERE " + strings.Join(exprs, " AND ")
	}

	// build filter
	if len(c.filter) > 0 {
		for _, v := range c.schema.ResolveColumnMap(c.target, c.filter) {
			if v.Value == nil {
				continue
			} else if v.IsString {
				exprs = append(exprs, c.schema.Quote(v.DbName)+" ILIKE "+c.params.get(v.Value.(string)+"%"))
			} else {
				exprs = append(exprs, c.schema.Quote(v.DbName)+" = "+c.params.get(v.Value))
			}
		}

		return " WHERE " + strings.Join(exprs, " AND ")
	}

	return ""
}
