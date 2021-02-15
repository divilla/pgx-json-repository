package pgxjrep

import (
	"context"
)

type DeleteStatement struct {
	builder         *Builder
	schema          *DbSchema
	target          string
	whereClause     *whereClause
	returningClause *returningClause
	params          *params
}

func (s *DeleteStatement) WhereStatement(statement string, args ...interface{}) *DeleteStatement {
	s.whereClause.statement = statement
	s.whereClause.statementArgs = args
	return s
}

func (s *DeleteStatement) Where(m map[string]interface{}) *DeleteStatement {
	s.whereClause.values = m
	return s
}

func (s *DeleteStatement) Returning(cols ...string) *DeleteStatement {
	s.returningClause.cols = cols
	return s
}

func (s *DeleteStatement) Build() (string, []interface{}) {
	var q = "DELETE FROM "
	q += s.schema.QuoteRelation(s.target)
	q += s.whereClause.build()
	q += s.returningClause.build()

	return q, s.params.args
}

func (s *DeleteStatement) Exec(conn PgxConn, ctx context.Context) (string, error) {
	sql, args := s.Build()
	return s.builder.Exec(conn, ctx, sql, args)
}

func (s *DeleteStatement) One(conn PgxConn, ctx context.Context) (string, error) {
	sql, args := s.Build()
	return s.builder.One(conn, ctx, sql, args)
}

func (s *DeleteStatement) OneMap(conn PgxConn, ctx context.Context) (map[string]interface{}, error) {
	sql, args := s.Build()
	return s.builder.OneMap(conn, ctx, sql, args)
}
