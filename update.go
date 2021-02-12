package pgxexec

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"strings"
)

type UpdateStatement struct {
	target        string
	setPlan       []FieldValue
	whereClause   *whereClause
	returning     interface{}
	returningPtrs []FieldValue
	params        *params
}

var UpdateWithoutSetValuesErr = errors.New("update without set values")

func Update(target string) *UpdateStatement {
	p := &params{}
	w := &whereClause{params: p}
	return &UpdateStatement{
		target:      target,
		whereClause: w,
		params:      p,
	}
}

func (s *UpdateStatement) Set(str interface{}) *UpdateStatement {
	for _, v := range FieldValues(str) {
		s.setPlan = append(s.setPlan, v)
	}

	return s
}

func (s *UpdateStatement) SetWherePrimaryKey(str interface{}) *UpdateStatement {
	for _, v := range FieldValues(str) {
		if v.PrimaryKey {
			s.WhereValuesAdd(v.ColumnName, v.Value)
		} else {
			s.setPlan = append(s.setPlan, v)
		}
	}

	return s
}

func (s *UpdateStatement) SetAdd(col string, value interface{}) *UpdateStatement {
	for k, v := range s.setPlan {
		if v.ColumnName == col {
			s.setPlan[k].Value = value
			return s
		}
	}

	s.setPlan = append(s.setPlan, FieldValue{
		ColumnName: col,
		PrimaryKey: false,
		Value:      value,
	})

	return s
}

func (s *UpdateStatement) WhereValues(values interface{}) *UpdateStatement {
	s.whereClause.valuesPlan = FieldValues(values)
	return s
}

func (s *UpdateStatement) WhereValuesAdd(col string, value interface{}) *UpdateStatement {
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
		PrimaryKey: false,
		Value:      value,
	})

	return s
}

//ptr parameter must be pointer
func (s *UpdateStatement) Returning(ptr interface{}) *UpdateStatement {
	for _, v := range FieldPointers(ptr) {
		s.returningPtrs = append(s.returningPtrs, v)
	}

	return s
}

func (s *UpdateStatement) ReturningSet(col string, ptr interface{}) *UpdateStatement {
	for k, v := range s.returningPtrs {
		if v.ColumnName == col {
			s.returningPtrs[k] = v
			return s
		}
	}

	s.returningPtrs = append(s.returningPtrs, FieldValue{
		ColumnName: col,
		PrimaryKey: false,
		Value:      ptr,
	})

	return s
}

func (s *UpdateStatement) Build() (string, []interface{}, error) {
	var q = "UPDATE "

	if s.target == "" {
		return "", nil, TargetRequiredErr
	}
	if len(s.setPlan) == 0 {
		return "", nil, UpdateWithoutSetValuesErr
	}

	q += QuoteRelationName(s.target) + " SET "

	var vals []string
	for _, v := range s.setPlan {
		if v.Value == nil {
			vals = append(vals, Quote(v.ColumnName)+" = NULL")
		} else {
			vals = append(vals, Quote(v.ColumnName)+" = "+s.params.get(v.Value))
		}
	}
	q += strings.Join(vals, ", ")

	c, err := s.whereClause.build()
	if err != nil {
		return "", nil, err
	}
	q += c

	if len(s.returningPtrs) > 0 {
		var rets []string
		for _, v := range s.returningPtrs {
			rets = append(rets, Quote(v.ColumnName))
		}
		q += " RETURNING " + strings.Join(rets, ", ")
	}

	return q, s.params.args, nil
}

func (s *UpdateStatement) Exec(conn PgxConn, ctx context.Context) (rowsAffected int64, err error) {
	sql, args, err := s.Build()
	if err != nil {
		return 0, err
	}

	ct, err := conn.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	if ct.RowsAffected() == 0 {
		return 0, pgx.ErrNoRows
	}

	return ct.RowsAffected(), nil
}

func (s *UpdateStatement) One(conn PgxConn, ctx context.Context) error {
	sql, args, err := s.Build()
	if err != nil {
		return err
	}

	if !strings.Contains(sql, "RETURNING") {
		ct, err := conn.Exec(ctx, sql, args...)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return pgx.ErrNoRows
		}

		return nil
	}

	ptrs := Pointers(s.returningPtrs)
	err = conn.QueryRow(ctx, sql, args...).Scan(ptrs...)
	if err != nil {
		return err
	}

	return nil
}

func (s *UpdateStatement) OneJson(conn PgxConn, ctx context.Context) (string, error) {
	sql, args, err := s.Build()
	if err != nil {
		return "", err
	}

	ind := strings.Index(sql, "RETURNING ")
	if ind == -1 {
		_, err = s.Exec(conn, ctx)
		return "", err
	}

	cols := strings.Split(sql[ind+10:], ", ")
	var colsNew []string
	for _, v := range cols {
		colsNew = append(colsNew, "'"+UnQuote(v)+"', "+v)
	}
	sqlNew := sql[0:ind+10] + "json_build_object(" + strings.Join(colsNew, ", ") + ")"

	jsn := new(string)

	err = conn.QueryRow(ctx, sqlNew, args...).Scan(jsn)
	if err != nil {
		return "", err
	}

	return *jsn, nil
}
