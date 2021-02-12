package pgxexec_test

import (
	"github.com/divilla/pgxexec"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"testing"
)

type deleteBuild struct {
	str  *pgxexec.DeleteStatement
	stm  string
	args []interface{}
	ret  int
	err  error
}

func TestDeleteBuild(t *testing.T) {
	buildResults := []deleteBuild{
		{str: pgxexec.Delete("test"), stm: "DELETE FROM test", args: nil},
		{str: pgxexec.Delete(""), stm: "", args: nil, err: pgxexec.TargetRequiredErr},
		{str: pgxexec.Delete("test").WhereValues(test1Insert),
			stm:  "DELETE FROM test WHERE a = $1 AND b = $2 AND c IS NULL",
			args: append(args, "a", 1)},
		{str: pgxexec.Delete("test.Test2").WhereValues(pk2Inst),
			stm:  "DELETE FROM test.\"Test2\" WHERE \"Id\" = $1",
			args: append(args, 1)},
		{str: pgxexec.Delete("test.Test2").WhereValues(test3Inst),
			stm:  "DELETE FROM test.\"Test2\" WHERE \"Y\" = $1 AND \"Z\" IS NULL",
			args: append(args, 1)},
	}

	for _, v := range buildResults {
		stm, argsOut, err := v.str.Build()
		assert.Equal(t, v.stm, stm)
		assert.Equal(t, v.args, argsOut)
		assert.Equal(t, v.err, err)
	}
}

func TestDeleteExec(t *testing.T) {
	if conn == nil {
		DB(t)
	}
	ResetTables(t, conn, "test1", "test.\"Test2\"")

	///
	ra, err := pgxexec.Insert("test1").Values(test1Insert).Exec(conn, ctx)
	assert.Equal(t, int64(1), ra)
	assert.Equal(t, nil, err)

	ra, err = pgxexec.Delete("test1").WhereValues(pk1Inst).Exec(conn, ctx)
	assert.Equal(t, int64(1), ra)
	assert.Equal(t, nil, err)

	res, err := pgxexec.Query("test1").WhereValues(pk1Inst).OneJson(conn, ctx)
	assert.Equal(t, "", res)
	assert.Equal(t, pgx.ErrNoRows, err)

	/////
	//ra, err = pgxexec.Insert("test.Test2").Values(test2Insert).Exec(conn, ctx)
	//assert.Equal(t, int64(1), ra)
	//assert.Equal(t, nil, err)
	//
	//ra, err = pgxexec.Delete("test.Test2").Set(test2Delete).WhereValues(pk2Inst).Exec(conn, ctx)
	//assert.Equal(t, int64(1), ra)
	//assert.Equal(t, nil, err)
	//
	//res, err = pgxexec.Query("test.Test2").WhereValues(pk2Inst).OneJson(conn, ctx)
	//assert.Equal(t, "{\"Id\":1,\"X\":\"f\",\"Y\":11,\"Z\":true}", res)
	//assert.Equal(t, nil, err)
	//
	/////
	//ra, err = pgxexec.Insert("test1").Values(test1Insert).Exec(conn, ctx)
	//assert.Equal(t, int64(1), ra)
	//assert.Equal(t, nil, err)
	//
	//err = pgxexec.Delete("test.Test2").SetWherePrimaryKey(test4DeletePk).Returning(&test2Ret).One(conn, ctx)
	//assert.Equal(t, "fff", test2Ret.X)
	//assert.Equal(t, 22, test2Ret.Y)
	//assert.Equal(t, true, test2Ret.Z)
	//assert.Equal(t, nil, err)
	//
	////
	//json, err := pgxexec.Delete("test.Test2").SetAdd("X", "S").ReturningSet("Id", &id).OneJson(conn, ctx)
	//assert.Equal(t, "{\"Id\" : 3}", json)
	//assert.Equal(t, nil, err)
}
