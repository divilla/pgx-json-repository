package pgxexec

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

type QueryStatement struct {
	target       string
	selectClause *selectClause
	whereClause  *whereClause
	orderBy      string
	limit        uint64
	offset       uint64
	params       *params
}

var TargetRequiredErr = errors.New("'target' is required parameter")

func Query(target string) *QueryStatement {
	p := &params{}
	w := &whereClause{params: p}
	q := &QueryStatement{
		target:       target,
		selectClause: &selectClause{},
		whereClause:  w,
		params:       p,
	}

	return q
}

//ptr argument must be pointer
func (s *QueryStatement) Select(ptr interface{}) *QueryStatement {
	s.selectClause.ptrStr = ptr
	for _, v := range FieldPointers(ptr) {
		v.ColumnName = Quote(v.ColumnName)
		s.selectClause.colPtrs = append(s.selectClause.colPtrs, v)
	}

	return s
}

func (s *QueryStatement) SelectAdd(column string, valuePtr interface{}, as ...string) *QueryStatement {
	asLen := len(as)
	for k, v := range s.selectClause.colPtrs {
		if v.ColumnName == column {
			s.selectClause.colPtrs[k].Value = valuePtr
			if asLen > 0 {
				s.selectClause.colPtrs[k].As = as[0]
			}
			return s
		}
	}

	var asValue string
	if asLen > 0 {
		asValue = as[0]
	}
	s.selectClause.colPtrs = append(s.selectClause.colPtrs, FieldValue{
		ColumnName: column,
		Value:      valuePtr,
		As:         asValue,
	})

	return s
}

func (s *QueryStatement) SelectDistinct() *QueryStatement {
	s.selectClause.distinct = true
	return s
}

func (s *QueryStatement) WhereStatement(statement string, args ...interface{}) *QueryStatement {
	s.whereClause.statement = statement
	s.whereClause.statementArgs = args
	return s
}

func (s *QueryStatement) WhereValues(values interface{}) *QueryStatement {
	s.whereClause.valuesPlan = FieldValues(values)
	return s
}

func (s *QueryStatement) WhereValueIs(col string, value interface{}) *QueryStatement {
	if len(s.whereClause.valuesPlan) > 0 {
		for k, v := range s.whereClause.valuesPlan {
			if v.ColumnName == col {
				s.whereClause.valuesPlan[k].Value = value
				return s
			}
		}
	}

	s.whereClause.valuesPlan = append(s.whereClause.valuesPlan, FieldValue{
		ColumnName: col,
		Value:      value,
	})

	return s
}

func (s *QueryStatement) WhereValueStartsWith(col string, value interface{}) *QueryStatement {
	if len(s.whereClause.valuesPlan) > 0 {
		for k, v := range s.whereClause.valuesPlan {
			if v.ColumnName == col {
				s.whereClause.valuesPlan[k].StartsWith = true
				s.whereClause.valuesPlan[k].Value = value
				return s
			}
		}
	}

	s.whereClause.valuesPlan = append(s.whereClause.valuesPlan, FieldValue{
		ColumnName: col,
		StartsWith: true,
		Value:      value,
	})

	return s
}

func (s *QueryStatement) WhereValueEndsWith(col string, value interface{}) *QueryStatement {
	if len(s.whereClause.valuesPlan) > 0 {
		for k, v := range s.whereClause.valuesPlan {
			if v.ColumnName == col {
				s.whereClause.valuesPlan[k].EndsWith = true
				s.whereClause.valuesPlan[k].Value = value
				return s
			}
		}
	}

	s.whereClause.valuesPlan = append(s.whereClause.valuesPlan, FieldValue{
		ColumnName: col,
		EndsWith:   true,
		Value:      value,
	})

	return s
}

func (s *QueryStatement) WhereValueContains(col string, value interface{}) *QueryStatement {
	if len(s.whereClause.valuesPlan) > 0 {
		for k, v := range s.whereClause.valuesPlan {
			if v.ColumnName == col {
				s.whereClause.valuesPlan[k].Contains = true
				s.whereClause.valuesPlan[k].Value = value
				return s
			}
		}
	}

	s.whereClause.valuesPlan = append(s.whereClause.valuesPlan, FieldValue{
		ColumnName: col,
		Contains:   true,
		Value:      value,
	})

	return s
}

func (s *QueryStatement) WhereFilter(values interface{}) *QueryStatement {
	s.whereClause.filterPlan = FieldValues(values)
	return s
}

func (s *QueryStatement) WhereFilterIs(col string, value interface{}) *QueryStatement {
	if len(s.whereClause.filterPlan) > 0 {
		for k, v := range s.whereClause.filterPlan {
			if v.ColumnName == col {
				s.whereClause.filterPlan[k].Value = value
				return s
			}
		}
	}

	s.whereClause.filterPlan = append(s.whereClause.filterPlan, FieldValue{
		ColumnName: col,
		Value:      value,
	})

	return s
}

func (s *QueryStatement) WhereFilterStartsWith(col string, value interface{}) *QueryStatement {
	if len(s.whereClause.filterPlan) > 0 {
		for k, v := range s.whereClause.filterPlan {
			if v.ColumnName == col {
				s.whereClause.filterPlan[k].StartsWith = true
				s.whereClause.filterPlan[k].Value = value
				return s
			}
		}
	}

	s.whereClause.filterPlan = append(s.whereClause.filterPlan, FieldValue{
		ColumnName: col,
		StartsWith: true,
		Value:      value,
	})

	return s
}

func (s *QueryStatement) WhereFilterEndsWith(col string, value interface{}) *QueryStatement {
	if len(s.whereClause.filterPlan) > 0 {
		for k, v := range s.whereClause.filterPlan {
			if v.ColumnName == col {
				s.whereClause.filterPlan[k].EndsWith = true
				s.whereClause.filterPlan[k].Value = value
				return s
			}
		}
	}

	s.whereClause.filterPlan = append(s.whereClause.filterPlan, FieldValue{
		ColumnName: col,
		EndsWith:   true,
		Value:      value,
	})

	return s
}

func (s *QueryStatement) WhereFilterContains(col string, value interface{}) *QueryStatement {
	if len(s.whereClause.filterPlan) > 0 {
		for k, v := range s.whereClause.filterPlan {
			if v.ColumnName == col {
				s.whereClause.filterPlan[k].Contains = true
				s.whereClause.filterPlan[k].Value = value
				return s
			}
		}
	}

	s.whereClause.filterPlan = append(s.whereClause.filterPlan, FieldValue{
		ColumnName: col,
		Contains:   true,
		Value:      value,
	})

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

func (s *QueryStatement) Build() (string, []interface{}, error) {
	var err error
	var c string
	var q = "SELECT"

	c, err = s.selectClause.build()
	if err != nil {
		return "", nil, err
	}
	q += c

	if s.target == "" {
		return "", nil, TargetRequiredErr
	}
	q += " FROM " + QuoteRelationName(s.target)

	c, err = s.whereClause.build()
	if err != nil {
		return "", nil, err
	}
	q += c

	if s.orderBy != "" {
		exps := strings.Split(s.orderBy, ",")
		var expsNew []string
		for _, v := range exps {
			flds := strings.Fields(v)
			if len(flds) > 1 && strings.ToUpper(flds[1]) == "DESC" {
				expsNew = append(expsNew, Quote(flds[0])+" DESC")
			} else if len(flds) > 0 {
				expsNew = append(expsNew, Quote(flds[0]))
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

	return q, s.params.args, nil
}

func (s *QueryStatement) All(conn PgxConn, ctx context.Context) error {
	jsn, err := s.AllJson(conn, ctx)
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(jsn), &s.selectClause.ptrStr)
	if err != nil {
		return err
	}

	return nil
}

func (s *QueryStatement) One(conn PgxConn, ctx context.Context) error {
	jsn, err := s.OneJson(conn, ctx)
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(jsn), &s.selectClause.ptrStr)
	if err != nil {
		return err
	}

	return nil
}

func (s *QueryStatement) Scalar(conn PgxConn, ctx context.Context) (interface{}, error) {
	sql, args, err := s.Build()
	if err != nil {
		return "", err
	}

	scalar := new(interface{})

	err = conn.QueryRow(ctx, sql, args...).Scan(scalar)
	if err != nil {
		return "", err
	}

	return *scalar, nil
}

func (s *QueryStatement) AllJson(conn PgxConn, ctx context.Context) (string, error) {
	sql, args, err := s.Build()
	if err != nil {
		return "", err
	}

	sql = "SELECT json_agg(t) as json FROM (" + sql + ") t;"
	jsn := new(string)

	err = conn.QueryRow(ctx, sql, args...).Scan(jsn)
	if err != nil {
		return "", err
	}

	return *jsn, nil
}

func (s *QueryStatement) OneJson(conn PgxConn, ctx context.Context) (string, error) {
	sql, args, err := s.Build()
	if err != nil {
		return "", err
	}

	sql = "SELECT row_to_json(t, false) as json FROM (" + sql + ") t;"
	jsn := new(string)

	err = conn.QueryRow(ctx, sql, args...).Scan(jsn)
	if err != nil {
		return "", err
	}

	return *jsn, nil
}

func (s *QueryStatement) Exists(conn PgxConn, ctx context.Context) (bool, error) {
	sql, args, err := s.Build()
	if err != nil {
		return false, err
	}

	sql = "SELECT exists(" + sql + ") as exists;"
	exists := new(bool)

	err = conn.QueryRow(ctx, sql, args...).Scan(exists)
	if err != nil {
		return false, err
	}

	return *exists, nil
}

func (s *QueryStatement) Count(conn PgxConn, ctx context.Context) (uint64, error) {
	sql, args, err := s.Build()
	if err != nil {
		return 0, err
	}

	fromInd := strings.Index(sql, "FROM")
	sql = "SELECT count(*) " + sql[fromInd:]
	count := new(uint64)

	err = conn.QueryRow(ctx, sql, args...).Scan(count)
	if err != nil {
		return 0, err
	}

	return *count, nil
}
