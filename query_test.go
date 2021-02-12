package pgxexec_test

import (
	"github.com/divilla/pgxexec"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"testing"
)

type queryBuild struct {
	str  *pgxexec.QueryStatement
	stm  string
	args []interface{}
	err  error
}

func TestQueryBuild(t *testing.T) {
	buildResults := []queryBuild{
		{str: pgxexec.Query("test"), stm: "SELECT * FROM test"},
		{str: pgxexec.Query(""), stm: "", args: nil, err: pgxexec.TargetRequiredErr},
		{str: pgxexec.Query("Test"), stm: "SELECT * FROM \"Test\""},
		{str: pgxexec.Query("public.Test"), stm: "SELECT * FROM \"Test\""},
		{str: pgxexec.Query("test.Test"), stm: "SELECT * FROM test.\"Test\""},
		{str: pgxexec.Query("Test.Test"), stm: "SELECT * FROM \"Test\".\"Test\""},
		{str: pgxexec.Query("test").Select(&testPGTInst), stm: "SELECT a, b, c FROM test"},
		{str: pgxexec.Query("test").SelectDistinct(), stm: "SELECT DISTINCT * FROM test"},
		{str: pgxexec.Query("test").WhereStatement("a = ? AND b = ? AND c = ?", "a", 1, nil),
			stm:  "SELECT * FROM test WHERE a = $1 AND b = $2 AND c = $3",
			args: append(args, "a", 1, nil)},
		{str: pgxexec.Query("test").WhereStatement("\"A\" = ? AND \"B\" = ? AND \"C\" = ?", "a", 1, nil),
			stm:  "SELECT * FROM test WHERE \"A\" = $1 AND \"B\" = $2 AND \"C\" = $3",
			args: append(args, "a", 1, nil)},
		{str: pgxexec.Query("test").WhereValues(test1Insert),
			stm:  "SELECT * FROM test WHERE a = $1 AND b = $2 AND c IS NULL",
			args: append(args, "a", 1)},
		{str: pgxexec.Query("test").WhereValues(&testPGTInst),
			stm:  "SELECT * FROM test WHERE a = $1 AND b = $2 AND c IS NULL",
			args: append(args, testPGTInst.A, testPGTInst.B)},
		{str: pgxexec.Query("test").WhereValues(&testPGTInst).WhereValueIs("a", pgtype.Text{String: "x", Status: pgtype.Present}),
			stm:  "SELECT * FROM test WHERE a = $1 AND b = $2 AND c IS NULL",
			args: append(args, pgtype.Text{String: "x", Status: pgtype.Present}, testPGTInst.B)},
		{str: pgxexec.Query("test").WhereFilter(test1Insert),
			stm:  "SELECT * FROM test WHERE a = $1 AND b = $2",
			args: append(args, "a", 1)},
		{str: pgxexec.Query("test").WhereFilter(&testPGTInst),
			stm:  "SELECT * FROM test WHERE a = $1 AND b = $2",
			args: append(args, testPGTInst.A, testPGTInst.B)},
		{str: pgxexec.Query("test").WhereFilter(&testPGTInst).WhereFilterIs("a", pgtype.Text{String: "x", Status: pgtype.Present}),
			stm:  "SELECT * FROM test WHERE a = $1 AND b = $2",
			args: append(args, pgtype.Text{String: "x", Status: pgtype.Present}, testPGTInst.B)},
		{str: pgxexec.Query("test").OrderBy("a, b desc"),
			stm: "SELECT * FROM test ORDER BY a, b DESC"},
		{str: pgxexec.Query("test").Limit(60).Offset(30),
			stm: "SELECT * FROM test LIMIT 60 OFFSET 30"},
	}

	for _, v := range buildResults {
		stm, argsOut, err := v.str.Build()
		assert.Equal(t, v.stm, stm)
		assert.Equal(t, v.args, argsOut)
		assert.Equal(t, v.err, err)
	}
}

func TestQueryExec(t *testing.T) {
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

	//
	test2Ret = test2Str{}
	err = pgxexec.Query("test.Test2").Select(&test2Ret).WhereValueIs("Id", 2).One(conn, ctx)
	assert.Equal(t, "fff", test2Ret.X)
	assert.Equal(t, 22, test2Ret.Y)
	assert.Equal(t, true, test2Ret.Z)
	assert.Equal(t, nil, err)

	//
	json, err := pgxexec.Query("test.Test2").SelectDistinct().OrderBy("Id").Offset(0).Limit(10).AllJson(conn, ctx)
	assert.Equal(t, "[{\"Id\":1,\"X\":\"f\",\"Y\":11,\"Z\":true}, \n {\"Id\":2,\"X\":\"fff\",\"Y\":22,\"Z\":true}]", json)
	assert.Equal(t, nil, err)
}
