package pgxjrep

import "strings"

type returningClause struct {
	schema *DbSchema
	target string
	cols   []string
}

func (c *returningClause) build() string {
	if len(c.cols) > 0 {
		var rets []string
		for _, v := range c.schema.ResolveColumns(c.target, c.cols) {
			rets = append(rets, c.schema.SingleQuote(v.JsonName)+", "+c.schema.Quote(v.DbName))
		}

		return " RETURNING json_build_object(" + strings.Join(rets, ", ") + ")"
	}

	return ""
}
