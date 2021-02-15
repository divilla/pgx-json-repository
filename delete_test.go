package pgxjrep_test

import (
	"github.com/divilla/pgxjrep"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"testing"
)

type deleteBuild struct {
	str  *pgxjrep.DeleteStatement
	stm  string
	args []interface{}
	ret  int
	err  error
}

func TestDeleteBuild(t *testing.T) {
	Init(t)

	buildResults := []deleteBuild{
		{str: builder.Delete("test1"), stm: "DELETE FROM test1", args: nil},
		{str: builder.Delete("test1").Where(pk1),
			stm:  "DELETE FROM test1 WHERE id = $1",
			args: append(args, 11)},
		{str: builder.Delete("test1").Where(insert1),
			stm:  "DELETE FROM test1 WHERE a_a LIKE $1 AND \"b_B\" = $2 AND cc_cc IS NULL",
			args: append(args, "a", 1)},
		{str: builder.Delete("test.Test2").Where(pk1),
			stm:  "DELETE FROM test.\"Test2\" WHERE \"Id\" = $1",
			args: append(args, 11)},
	}

	for _, v := range buildResults {
		stm, argsOut := v.str.Build()
		assert.Equal(t, v.stm, stm)
		assert.Equal(t, v.args, argsOut)
	}
}

func TestDeleteExec(t *testing.T) {
	Init(t)
	ResetTables(t, conn, "test1", "test.\"Test2\"")

	//test 1
	json, err := builder.Insert("test1").Values(insert1).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = builder.Delete("test1").Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = builder.Query("test1").All(conn, ctx)
	assert.Equal(t, "[]", json)
	assert.Equal(t, nil, err)

	//test 2
	json, err = builder.Insert("test1").Values(insert1).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = builder.Insert("test1").Values(insert1).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	pk2 := map[string]interface{}{
		"id": 2,
	}
	json, err = builder.Delete("test1").Where(pk2).Returning("id").One(conn, ctx)
	assert.Equal(t, "{\"id\" : 2}", json)
	assert.Equal(t, nil, err)

	pk3 := map[string]interface{}{
		"id": 3,
	}
	json, err = builder.Query("test1").Where(pk3).One(conn, ctx)
	assert.Equal(t, "{\"id\":3,\"aA\":\"a\",\"bB\":1,\"ccCc\":true}", json)
	assert.Equal(t, nil, err)
}
