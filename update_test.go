package pgxjrep_test

import (
	"github.com/divilla/pgxjrep"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"testing"
)

type updateBuild struct {
	str  *pgxjrep.UpdateStatement
	stm  string
	args []interface{}
	ret  int
	err  error
}

func TestUpdateBuild(t *testing.T) {
	Init(t)

	buildResults := []updateBuild{
		//{str: builder.Update("test"), stm: "", args: nil, err: pgxjrep.UpdateWithoutSetValuesErr},
		{str: builder.Update("test1").Set(insert1),
			stm:  "UPDATE test1 SET a_a = $1, \"b_B\" = $2, cc_cc = NULL",
			args: append(args, "a", 1)},
		{str: builder.Update("test1").Set(insert2),
			stm:  "UPDATE test1 SET a_a = $1, \"b_B\" = $2, cc_cc = NULL",
			args: append(args, "a", 1)},
		{str: builder.Update("test.Test2").Set(insert3),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = NULL",
			args: append(args, "a", 1)},
		{str: builder.Update("test.Test2").Set(insert4),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = NULL",
			args: append(args, "c", 3)},
		{str: builder.Update("test1").Set(insert1).Where(pk1),
			stm:  "UPDATE test1 SET a_a = $1, \"b_B\" = $2, cc_cc = NULL WHERE id = $3",
			args: append(args, "a", 1, 11)},
		{str: builder.Update("test.Test2").Set(insert3).Where(pk1),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = NULL WHERE \"Id\" = $3",
			args: append(args, "a", 1, 11)},
		{str: builder.Update("test1").SetWherePk(update1),
			stm:  "UPDATE test1 SET a_a = $1, \"b_B\" = $2, cc_cc = NULL WHERE id = $3",
			args: append(args, "a", 1, 22)},
		{str: builder.Update("test1").SetWherePk(update2),
			stm:  "UPDATE test1 SET a_a = $1, \"b_B\" = $2, cc_cc = NULL WHERE id = $3",
			args: append(args, "a", 1, 22)},
		{str: builder.Update("test.Test2").SetWherePk(update3),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = NULL WHERE \"Id\" = $3",
			args: append(args, "a", 1, 22)},
		{str: builder.Update("test.Test2").SetWherePk(update4),
			stm:  "UPDATE test.\"Test2\" SET \"X\" = $1, \"Y\" = $2, \"Z\" = NULL WHERE \"Id\" = $3",
			args: append(args, "a", 1, 22)},
		{str: builder.Update("test1").SetWherePk(update1).Returning("id"),
			stm:  "UPDATE test1 SET a_a = $1, \"b_B\" = $2, cc_cc = NULL WHERE id = $3 RETURNING json_build_object('id', id)",
			args: append(args, "a", 1, 22)},
		{str: builder.Update("test1").SetWherePk(update1).Returning("id", "aA"),
			stm:  "UPDATE test1 SET a_a = $1, \"b_B\" = $2, cc_cc = NULL WHERE id = $3 RETURNING json_build_object('id', id, 'aA', a_a)",
			args: append(args, "a", 1, 22)},
	}

	for _, v := range buildResults {
		stm, argsOut := v.str.Build()
		assert.Equal(t, v.stm, stm)
		assert.Equal(t, v.args, argsOut)
	}
}

func TestUpdateExec(t *testing.T) {
	Init(t)
	ResetTables(t, conn, "test1", "test.\"Test2\"")

	//test 1
	json, err := builder.Insert("test1").Values(insert1).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	u1 := map[string]interface{}{
		"a_a": "c",
	}
	json, err = builder.Update("test1").Set(u1).Returning("aA").One(conn, ctx)
	assert.Equal(t, "{\"aA\" : \"c\"}", json)
	assert.Equal(t, nil, err)

	//test 2
	json, err = builder.Insert("test1").Values(insert1).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	var pk2 = map[string]interface{}{
		"id": 1,
	}
	u2 := map[string]interface{}{
		"aA": "f",
		"bB": 33,
	}
	json, err = builder.Update("test1").Set(u2).Where(pk2).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = builder.Query("test1").Where(pk2).One(conn, ctx)
	assert.Equal(t, "{\"id\":1,\"aA\":\"f\",\"bB\":33,\"ccCc\":true}", json)
	assert.Equal(t, nil, err)

	//test 3
	json, err = builder.Insert("test.Test2").Values(insert3).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	var pk3 = map[string]interface{}{
		"id": 1,
	}
	u3 := map[string]interface{}{
		"id": 1,
		"x":  "f",
		"y":  33,
		"t":  99,
	}
	res, err := builder.Update("test.Test2").SetWherePk(u3).Returning("id", "t").OneMap(conn, ctx)
	assert.Equal(t, float64(1), res["id"].(float64))
	assert.Equal(t, nil, err)

	json, err = builder.Query("test.Test2").Select("id", "x", "y", "t").Where(pk3).One(conn, ctx)
	assert.Equal(t, "{\"id\":1,\"x\":\"f\",\"y\":33}", json)
	assert.Equal(t, nil, err)
}
