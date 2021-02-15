package pgxjrep

import (
	"context"
	"strings"
)

type InsertStatement struct {
	builder         *Builder
	schema          *DbSchema
	target          string
	values          map[string]interface{}
	returningClause *returningClause
	params          *params
}

func (s *InsertStatement) Values(m map[string]interface{}) *InsertStatement {
	s.values = m
	return s
}

func (s *InsertStatement) ValueSet(col string, value interface{}) *InsertStatement {
	s.values[col] = value
	return s
}

func (s *InsertStatement) Returning(cols ...string) *InsertStatement {
	s.returningClause.cols = cols
	return s
}

func (s *InsertStatement) Build() (string, []interface{}) {
	var q = "INSERT"

	q += " INTO " + s.schema.QuoteRelation(s.target)

	if len(s.values) > 0 {
		var cols, vals []string

		for _, v := range s.schema.ResolveColumnMap(s.target, s.values) {
			if v.Value == nil {
				continue
			} else {
				cols = append(cols, s.schema.Quote(v.DbName))
				vals = append(vals, s.params.get(v.Value))
			}
		}

		q += " (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")"
	} else {
		q += " DEFAULT VALUES"
	}

	q += s.returningClause.build()

	return q, s.params.args
}

func (s *InsertStatement) Exec(conn PgxConn, ctx context.Context) (string, error) {
	sql, args := s.Build()
	return s.builder.Exec(conn, ctx, sql, args)
}

func (s *InsertStatement) One(conn PgxConn, ctx context.Context) (string, error) {
	sql, args := s.Build()
	return s.builder.One(conn, ctx, sql, args)
}

func (s *InsertStatement) OneMap(conn PgxConn, ctx context.Context) (map[string]interface{}, error) {
	sql, args := s.Build()
	return s.builder.OneMap(conn, ctx, sql, args)
}
