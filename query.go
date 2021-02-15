package pgxjrep

import (
	"context"
	"github.com/jackc/pgtype"
	"strconv"
	"strings"
)

type QueryStatement struct {
	builder     *Builder
	schema      *DbSchema
	target      string
	selectCols  []string
	distinct    bool
	whereClause *whereClause
	orderBy     string
	limit       uint64
	offset      uint64
	params      *params
}

func (s *QueryStatement) Distinct() *QueryStatement {
	s.distinct = true
	return s
}

func (s *QueryStatement) Select(c ...string) *QueryStatement {
	s.selectCols = c
	return s
}

func (s *QueryStatement) WhereStatement(statement string, args ...interface{}) *QueryStatement {
	s.whereClause.statement = statement
	s.whereClause.statementArgs = args
	return s
}

func (s *QueryStatement) Where(m map[string]interface{}) *QueryStatement {
	s.whereClause.values = m
	return s
}

func (s *QueryStatement) Filter(m map[string]interface{}) *QueryStatement {
	s.whereClause.filter = m
	return s
}

func (s *QueryStatement) OrderBy(clause string) *QueryStatement {
	s.orderBy = clause
	return s
}

func (s *QueryStatement) Limit(val uint64) *QueryStatement {
	s.limit = val
	return s
}

func (s *QueryStatement) Offset(val uint64) *QueryStatement {
	s.offset = val
	return s
}

func (s *QueryStatement) Build() (string, []interface{}) {
	var q = "SELECT"

	if s.distinct {
		q += " DISTINCT"
	}

	if len(s.selectCols) > 0 {
		var cols []string
		for _, v := range s.schema.ResolveColumns(s.target, s.selectCols) {
			if v.DbName == v.JsonName {
				cols = append(cols, s.schema.Quote(v.DbName))
			} else {
				cols = append(cols, s.schema.Quote(v.DbName)+" AS "+s.schema.Quote(v.JsonName))
			}
		}
		q += " " + strings.Join(cols, ", ")
	} else {
		var cols []string
		for _, v := range s.schema.ColSchema(s.target) {
			json := s.schema.ToJsonCase(v.ColumnName)
			if v.ColumnName == json {
				cols = append(cols, s.schema.Quote(v.ColumnName))
			} else {
				cols = append(cols, s.schema.Quote(v.ColumnName)+" AS "+s.schema.Quote(json))
			}
		}
		q += " " + strings.Join(cols, ", ")
	}

	q += " FROM " + s.schema.QuoteRelation(s.target)
	q += s.whereClause.build()

	if s.orderBy != "" {
		exps := strings.Split(s.orderBy, ",")
		var expsNew []string
		for _, v := range exps {
			fls := strings.Fields(v)
			if len(fls) > 1 && strings.ToUpper(fls[1]) == "DESC" {
				expsNew = append(expsNew, s.schema.Quote(fls[0])+" DESC")
			} else if len(fls) > 0 {
				expsNew = append(expsNew, s.schema.Quote(fls[0]))
			}
		}
		q += " ORDER BY " + strings.Join(expsNew, ", ")
	}

	if s.limit > 0 {
		q += " LIMIT " + strconv.FormatUint(s.limit, 10)
	}

	if s.offset > 0 {
		q += " OFFSET " + strconv.FormatUint(s.offset, 10)
	}

	return q, s.params.args
}

func (s *QueryStatement) All(conn PgxConn, ctx context.Context) (string, error) {
	sql, args := s.Build()

	sql = "SELECT json_agg(t) as json FROM (" + sql + ") t;"

	json := new(pgtype.Text)
	err := conn.QueryRow(ctx, sql, args...).Scan(json)
	if err != nil {
		return "", err
	}
	if json.Status == pgtype.Null {
		return "[]", err
	}

	return json.String, nil
}

func (s *QueryStatement) One(conn PgxConn, ctx context.Context) (string, error) {
	sql, args := s.Build()

	sql = "SELECT row_to_json(t, false) as json FROM (" + sql + ") t;"
	jsn := new(string)

	err := conn.QueryRow(ctx, sql, args...).Scan(jsn)
	if err != nil {
		return "", err
	}

	return *jsn, nil
}

func (s *QueryStatement) Scalar(conn PgxConn, ctx context.Context) (interface{}, error) {
	sql, args := s.Build()

	scalar := new(interface{})
	err := conn.QueryRow(ctx, sql, args...).Scan(scalar)
	if err != nil {
		return "", err
	}

	return *scalar, nil
}

func (s *QueryStatement) Exists(conn PgxConn, ctx context.Context) (bool, error) {
	sql, args := s.Build()
	sql = "SELECT EXISTS(" + sql + ") as exists;"

	exists := new(bool)
	err := conn.QueryRow(ctx, sql, args...).Scan(exists)
	if err != nil {
		return false, err
	}

	return *exists, nil
}

func (s *QueryStatement) Count(conn PgxConn, ctx context.Context) (uint64, error) {
	sql, args := s.Build()
	fromInd := strings.Index(sql, "FROM")
	sql = "SELECT COUNT(*) " + sql[fromInd:]

	count := new(uint64)
	err := conn.QueryRow(ctx, sql, args...).Scan(count)
	if err != nil {
		return 0, err
	}

	return *count, nil
}
