package pgxexec_test

import (
	"github.com/divilla/pgxexec"
	"github.com/stretchr/testify/assert"
	"testing"
)

type updateBuild struct {
	str  *pgxexec.UpdateStatement
	stm  string
	args []interface{}
	ret  int
	err  error
}

func TestUpdateBuild(t *testing.T) {
	buildResults := []updateBuild{
		{str: pgxexec.Update("test"), stm: "", args: nil, err: pgxexec.UpdateWithoutSetValuesErr},
		{str: pgxexec.Update(""), stm: "", args: nil, err: pgxexec.TargetRequiredErr},
		{str: pgxexec.Update("test").Set(test1Insert),
			stm:  "UPDATE test SET a = $1, b = $2, c = NULL",
			args: append(args, "a", 1)},
		{str: pgxexec.Update("test").Set(test1Insert).WhereValues(pk1Inst),
			stm:  "UPDATE test SET a = $1, b = $2, c = NULL WHERE id = $3",
			args: append(args, "a", 1, 1)},
		{str: pgxexec.Update("test.Test2").Set(test2Insert).WhereValues(pk2Inst),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = NULL WHERE \"Id\" = $3",
			args: append(args, "a", 1, 1)},
		{str: pgxexec.Update("test.Test2").SetWherePrimaryKey(test4UpdatePk),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = $3 WHERE \"Id\" = $4",
			args: append(args, "fff", 22, true, 2)},
		{str: pgxexec.Update("test").Set(test1Insert).WhereValuesAdd("a", "a").WhereValuesAdd("c", nil),
			stm:  "UPDATE test SET a = $1, b = $2, c = NULL WHERE a = $3 AND c IS NULL",
			args: append(args, "a", 1, "a")},
		{str: pgxexec.Update("test.Test2").Set(test2Insert).WhereValues(test3Inst),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = NULL WHERE \"Y\" = $3 AND \"Z\" IS NULL",
			args: append(args, "a", 1, 1)},
		{str: pgxexec.Update("test.Test2").SetWherePrimaryKey(test4UpdatePk).Returning(&test4UpdatePk),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = $3 WHERE \"Id\" = $4 RETURNING \"Id\", \"X\", \"Y\", \"Z\"",
			args: append(args, "fff", 22, true, 2)},
		{str: pgxexec.Update("test.Test2").SetWherePrimaryKey(test4UpdatePk).ReturningSet("Id", &id),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = $3 WHERE \"Id\" = $4 RETURNING \"Id\"",
			args: append(args, "fff", 22, true, 2)},
	}

	for _, v := range buildResults {
		stm, argsOut, err := v.str.Build()
		assert.Equal(t, v.stm, stm)
		assert.Equal(t, v.args, argsOut)
		assert.Equal(t, v.err, err)
	}
}

func TestUpdateExec(t *testing.T) {
	if conn == nil {
		DB(t)
	}
	ResetTables(t, conn, "test1", "test.\"Test2\"")

	///
	ra, err := pgxexec.Insert("test1").Values(test1Insert).Exec(conn, ctx)
	assert.Equal(t, int64(1), ra)
	assert.Equal(t, nil, err)

	ra, err = pgxexec.Update("test1").Set(test1Update).WhereValues(pk1Inst).Exec(conn, ctx)
	assert.Equal(t, int64(1), ra)
	assert.Equal(t, nil, err)

	res, err := pgxexec.Query("test1").WhereValues(pk1Inst).OneJson(conn, ctx)
	assert.Equal(t, "{\"id\":1,\"a\":\"f\",\"b\":11,\"c\":true}", res)
	assert.Equal(t, nil, err)

	///
	ra, err = pgxexec.Insert("test.Test2").Values(test2Insert).Exec(conn, ctx)
	assert.Equal(t, int64(1), ra)
	assert.Equal(t, nil, err)

	ra, err = pgxexec.Update("test.Test2").Set(test2Update).WhereValues(pk2Inst).Exec(conn, ctx)
	assert.Equal(t, int64(1), ra)
	assert.Equal(t, nil, err)

	res, err = pgxexec.Query("test.Test2").WhereValues(pk2Inst).OneJson(conn, ctx)
	assert.Equal(t, "{\"Id\":1,\"X\":\"f\",\"Y\":11,\"Z\":true}", res)
	assert.Equal(t, nil, err)

	///
	ra, err = pgxexec.Insert("test.Test2").Values(test2Insert).Exec(conn, ctx)
	assert.Equal(t, int64(1), ra)
	assert.Equal(t, nil, err)

	err = pgxexec.Update("test.Test2").SetWherePrimaryKey(test4UpdatePk).Returning(&test2Ret).One(conn, ctx)
	assert.Equal(t, "fff", test2Ret.X)
	assert.Equal(t, 22, test2Ret.Y)
	assert.Equal(t, true, test2Ret.Z)
	assert.Equal(t, nil, err)

	err = pgxexec.Update("test.Test2").SetWherePrimaryKey(test4UpdatePk).One(conn, ctx)
	assert.Equal(t, nil, err)

	//
	json, err := pgxexec.Update("test.Test2").SetAdd("X", "S").WhereValuesAdd("Id", 1).ReturningSet("Id", &id).OneJson(conn, ctx)
	assert.Equal(t, "{\"Id\" : 1}", json)
	assert.Equal(t, nil, err)
}
