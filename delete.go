package pgxexec

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"strings"
)

type DeleteStatement struct {
	target        string
	whereClause   *whereClause
	returning     interface{}
	returningPtrs []FieldValue
	params        *params
}

func Delete(target string) *DeleteStatement {
	p := &params{}
	w := &whereClause{params: p}
	return &DeleteStatement{
		target:      target,
		whereClause: w,
		params:      p,
	}
}

func (s *DeleteStatement) WhereValues(values interface{}) *DeleteStatement {
	s.whereClause.valuesPlan = FieldValues(values)
	return s
}

func (s *DeleteStatement) WhereValuesAdd(col string, value interface{}) *DeleteStatement {
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
func (s *DeleteStatement) Returning(ptr interface{}) *DeleteStatement {
	for _, v := range FieldPointers(ptr) {
		s.returningPtrs = append(s.returningPtrs, v)
	}

	return s
}

func (s *DeleteStatement) ReturningSet(col string, ptr interface{}) *DeleteStatement {
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

func (s *DeleteStatement) Build() (string, []interface{}, error) {
	var q = "DELETE FROM "

	if s.target == "" {
		return "", nil, TargetRequiredErr
	}
	q += QuoteRelationName(s.target)

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

func (s *DeleteStatement) Exec(conn PgxConn, ctx context.Context) (rowsAffected int64, err error) {
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

func (s *DeleteStatement) One(conn PgxConn, ctx context.Context) error {
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

func (s *DeleteStatement) OneJson(conn PgxConn, ctx context.Context) (string, error) {
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

var UpdatedBeforeDeleteError = errors.New("Please check changes before you delete again")

func (s *DeleteStatement) Version(conn PgxConn, ctx context.Context) error {
	sql, args, err := s.Build()
	if err != nil {
		return err
	}

	ct, errex := conn.Exec(ctx, sql, args...)
	if errex != nil {
		return errex
	}
	if ct.RowsAffected() == 0 {
		stmt := Query("taxonomy_group").SelectAdd("name_username(modified_by)", "fullName")
		for _, v := range s.whereClause.valuesPlan {
			if v.PrimaryKey {
				stmt.WhereValueIs(v.ColumnName, v.Value)
			}
		}
		res, err := stmt.Scalar(conn, ctx)
		if err != nil {
			return err
		}

		return errors.WithMessage(UpdatedBeforeDeleteError, res.(string)+" just modified these data")
	}

	return nil
}
