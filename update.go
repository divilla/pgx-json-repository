package pgxjrep

import (
	"context"
	"strings"
)

type UpdateStatement struct {
	builder         *Builder
	schema          *DbSchema
	target          string
	values          map[string]interface{}
	valWhrPk        bool
	whereClause     *whereClause
	returningClause *returningClause
	params          *params
}

func (s *UpdateStatement) Set(m map[string]interface{}) *UpdateStatement {
	s.values = m
	return s
}

func (s *UpdateStatement) SetWherePk(m map[string]interface{}) *UpdateStatement {
	s.values = m
	s.valWhrPk = true
	return s
}

func (s *UpdateStatement) WhereStatement(statement string, args ...interface{}) *UpdateStatement {
	s.whereClause.statement = statement
	s.whereClause.statementArgs = args
	return s
}

func (s *UpdateStatement) Where(m map[string]interface{}) *UpdateStatement {
	s.whereClause.values = m
	return s
}

func (s *UpdateStatement) Filter(m map[string]interface{}) *UpdateStatement {
	s.whereClause.values = m
	return s
}

func (s *UpdateStatement) Returning(cols ...string) *UpdateStatement {
	s.returningClause.cols = cols
	return s
}

func (s *UpdateStatement) Build() (string, []interface{}) {
	var q = "UPDATE "

	q += s.schema.QuoteRelation(s.target) + " SET "

	var vals []string
	for _, v := range s.schema.ResolveColumnMap(s.target, s.values) {
		if s.valWhrPk && v.IsPk {
			s.whereClause.colData = append(s.whereClause.colData, v)
		} else if v.Value == nil {
			vals = append(vals, s.schema.Quote(v.DbName)+" = NULL")
		} else {
			vals = append(vals, s.schema.Quote(v.DbName)+" = "+s.params.get(v.Value))
		}
	}
	q += strings.Join(vals, ", ")

	q += s.whereClause.build()
	q += s.returningClause.build()

	return q, s.params.args
}

func (s *UpdateStatement) Exec(conn PgxConn, ctx context.Context) (string, error) {
	sql, args := s.Build()
	return s.builder.Exec(conn, ctx, sql, args)
}

func (s *UpdateStatement) One(conn PgxConn, ctx context.Context) (string, error) {
	sql, args := s.Build()
	return s.builder.One(conn, ctx, sql, args)
}

func (s *UpdateStatement) OneMap(conn PgxConn, ctx context.Context) (map[string]interface{}, error) {
	sql, args := s.Build()
	return s.builder.OneMap(conn, ctx, sql, args)
}
