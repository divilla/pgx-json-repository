package pgxjrep

import (
	"context"
	"fmt"
	"math"
)

type Repository struct {
	builder *Builder
	conn    PgxConn
	ctx     context.Context
}

func New(builder *Builder, conn PgxConn, ctx context.Context) *Repository {
	return &Repository{
		builder: builder,
		conn:    conn,
		ctx:     ctx,
	}
}

func (r *Repository) All(target string) (string, error) {
	return r.builder.Query(target).All(r.conn, r.ctx)
}

func (r *Repository) Filter(target string, values map[string]interface{}, orderBy string, page uint64, pageSize uint64) (string, error) {
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * pageSize
	return r.builder.Query(target).
		Filter(values).
		OrderBy(orderBy).
		Offset(offset).
		Limit(pageSize).
		All(r.conn, r.ctx)
}

func (r *Repository) Pages(target string, values map[string]interface{}, pageSize uint64) (string, error) {
	cnt, err := r.builder.Query(target).Filter(values).Count(r.conn, r.ctx)
	if err != nil {
		return "", err
	}

	var pages float64
	if cnt != 0 {
		pages = math.Ceil(float64(cnt / pageSize))
	}

	return fmt.Sprintf("{\"pages\": %v}", pages), nil
}

func (r *Repository) OneByPk(target string, pk map[string]interface{}) (string, error) {
	return r.builder.Query(target).Where(pk).One(r.conn, r.ctx)
}

func (r *Repository) Insert(target string, values map[string]interface{}, returning ...string) (string, error) {
	if len(returning) > 0 {
		return r.builder.Insert(target).Values(values).Returning(returning...).One(r.conn, r.ctx)
	}
	return r.builder.Insert(target).Values(values).Exec(r.conn, r.ctx)
}

func (r *Repository) Update(target string, values map[string]interface{}, returning ...string) (string, error) {
	if len(returning) > 0 {
		return r.builder.Update(target).SetWherePk(values).Returning(returning...).One(r.conn, r.ctx)
	}
	return r.builder.Update(target).SetWherePk(values).Exec(r.conn, r.ctx)
}

func (r *Repository) Delete(target string, values map[string]interface{}, returning ...string) (string, error) {
	if len(returning) > 0 {
		return r.builder.Delete(target).Where(values).Returning(returning...).One(r.conn, r.ctx)
	}
	return r.builder.Delete(target).Where(values).Exec(r.conn, r.ctx)
}
