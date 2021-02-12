package pgxexec

import (
	"context"
	"github.com/jackc/pgx/v4"
	"strings"
)

type InsertStatement struct {
	target        string
	valuesPlan    []FieldValue
	returning     interface{}
	returningPtrs []FieldValue
	params        *params
}

func Insert(target string) *InsertStatement {
	return &InsertStatement{
		target: target,
		params: &params{},
	}
}

func (s *InsertStatement) Values(str interface{}) *InsertStatement {
	for _, v := range FieldValues(str) {
		s.valuesPlan = append(s.valuesPlan, v)
	}

	return s
}

func (s *InsertStatement) ValueSet(col string, value interface{}) *InsertStatement {
	for k, v := range s.valuesPlan {
		if v.ColumnName == col {
			s.valuesPlan[k].Value = value
			return s
		}
	}

	s.valuesPlan = append(s.valuesPlan, FieldValue{
		ColumnName: col,
		PrimaryKey: false,
		Value:      value,
	})

	return s
}

//ptr parameter must be pointer
func (s *InsertStatement) Returning(ptr interface{}) *InsertStatement {
	for _, v := range FieldPointers(ptr) {
		s.returningPtrs = append(s.returningPtrs, v)
	}

	return s
}

func (s *InsertStatement) ReturningAdd(col string, ptr interface{}) *InsertStatement {
	for k, v := range s.returningPtrs {
		if v.ColumnName == col {
			s.returningPtrs[k].Value = ptr
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

func (s *InsertStatement) Build() (string, []interface{}, error) {
	var q = "INSERT"

	if s.target == "" {
		return "", nil, TargetRequiredErr
	}
	q += " INTO " + QuoteRelationName(s.target)

	if len(s.valuesPlan) > 0 {
		var cols, vals []string
		for _, v := range s.valuesPlan {
			if v.Value == nil {
				continue
			}

			cols = append(cols, Quote(v.ColumnName))
			vals = append(vals, s.params.get(v.Value))
		}
		q += " (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")"
	} else {
		q += " DEFAULT VALUES"
	}

	if len(s.returningPtrs) > 0 {
		var rets []string
		for _, v := range s.returningPtrs {
			rets = append(rets, Quote(v.ColumnName))
		}
		q += " RETURNING " + strings.Join(rets, ", ")
	}

	return q, s.params.args, nil
}

func (s *InsertStatement) Exec(conn PgxConn, ctx context.Context) (rowsAffected int64, err error) {
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

func (s *InsertStatement) One(conn PgxConn, ctx context.Context) error {
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

func (s *InsertStatement) OneJson(conn PgxConn, ctx context.Context) (string, error) {
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
