package pgxjrep

import (
	"context"
	"fmt"
	"github.com/tidwall/gjson"
)

type Builder struct {
	*DbSchema
}

func NewBuilder(conn PgxConn, ctx context.Context) (*Builder, error) {
	dbSchema, err := NewSchema(conn, ctx)
	if err != nil {
		return nil, err
	}

	return &Builder{
		DbSchema: dbSchema,
	}, nil
}

func (b *Builder) Query(target string) *QueryStatement {
	p := &params{}
	q := &QueryStatement{
		builder: b,
		schema:  b.DbSchema,
		target:  target,
		whereClause: &whereClause{
			schema:        b.DbSchema,
			target:        target,
			statementArgs: make([]interface{}, 0),
			values:        make(map[string]interface{}),
			filter:        make(map[string]interface{}),
			params:        p,
		},
		params: p,
	}

	return q
}

func (b *Builder) Insert(target string) *InsertStatement {
	return &InsertStatement{
		builder: b,
		schema:  b.DbSchema,
		target:  target,
		returningClause: &returningClause{
			schema: b.DbSchema,
			target: target,
		},
		params: &params{},
	}
}

func (b *Builder) Update(target string) *UpdateStatement {
	p := &params{}
	return &UpdateStatement{
		builder: b,
		schema:  b.DbSchema,
		target:  target,
		values:  make(map[string]interface{}),
		whereClause: &whereClause{
			schema: b.DbSchema,
			target: target,
			values: make(map[string]interface{}),
			filter: make(map[string]interface{}),
			params: p,
		},
		returningClause: &returningClause{
			schema: b.DbSchema,
			target: target,
		},
		params: p,
	}
}

func (b *Builder) Delete(target string) *DeleteStatement {
	p := &params{}
	return &DeleteStatement{
		builder: b,
		schema:  b.DbSchema,
		target:  target,
		whereClause: &whereClause{
			schema: b.DbSchema,
			target: target,
			values: make(map[string]interface{}),
			filter: make(map[string]interface{}),
			params: p,
		},
		returningClause: &returningClause{
			schema: b.DbSchema,
			target: target,
		},
		params: p,
	}
}

func (b *Builder) Exec(conn PgxConn, ctx context.Context, sql string, args []interface{}) (string, error) {
	ct, err := conn.Exec(ctx, sql, args...)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("{\"rowsAffected\": %v}", ct.RowsAffected()), nil
}

func (b *Builder) One(conn PgxConn, ctx context.Context, sql string, args []interface{}) (string, error) {
	json := new(string)
	err := conn.QueryRow(ctx, sql, args...).Scan(json)
	if err != nil {
		return "", err
	}

	return *json, nil
}

func (b *Builder) OneMap(conn PgxConn, ctx context.Context, sql string, args []interface{}) (map[string]interface{}, error) {
	json, err := b.One(conn, ctx, sql, args)
	if err != nil {
		return nil, err
	}

	return gjson.Parse(json).Value().(map[string]interface{}), nil
}
