package pgxjrep_test

import (
	"github.com/divilla/pgxjrep"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"testing"
)

type insertBuild struct {
	str  *pgxjrep.InsertStatement
	stm  string
	args []interface{}
	ret  int
	err  error
}

func TestInsertBuild(t *testing.T) {
	Init(t)

	buildResults := []insertBuild{
		{str: builder.Insert("test1"), stm: "INSERT INTO test1 DEFAULT VALUES"},
		{str: builder.Insert("test1").Values(insert1),
			stm:  "INSERT INTO test1 (a_a, \"b_B\") VALUES ($1, $2)",
			args: append(args, "a", 1)},
		{str: builder.Insert("test1").Values(insert2),
			stm:  "INSERT INTO test1 (a_a, \"b_B\") VALUES ($1, $2)",
			args: append(args, "a", 1)},
		{str: builder.Insert("test.Test2").Values(insert3),
			stm:  "INSERT INTO test.\"Test2\" (\"X\", \"Y\") VALUES ($1, $2)",
			args: append(args, "a", 1)},
		{str: builder.Insert("test.Test2").Values(insert4),
			stm:  "INSERT INTO test.\"Test2\" (\"X\", \"Y\") VALUES ($1, $2)",
			args: append(args, "c", 3)},
		{str: builder.Insert("test1").Values(insert1).Returning("id", "a_a"),
			stm:  "INSERT INTO test1 (a_a, \"b_B\") VALUES ($1, $2) RETURNING json_build_object('id', id, 'aA', a_a)",
			args: append(args, "a", 1)},
		{str: builder.Insert("test1").Values(insert1).Returning("id", "aA"),
			stm:  "INSERT INTO test1 (a_a, \"b_B\") VALUES ($1, $2) RETURNING json_build_object('id', id, 'aA', a_a)",
			args: append(args, "a", 1)},
		{str: builder.Insert("test.Test2").Values(insert3).Returning("Id", "X"),
			stm:  "INSERT INTO test.\"Test2\" (\"X\", \"Y\") VALUES ($1, $2) RETURNING json_build_object('id', \"Id\", 'x', \"X\")",
			args: append(args, "a", 1)},
		{str: builder.Insert("test.Test2").Values(insert3).Returning("id", "x"),
			stm:  "INSERT INTO test.\"Test2\" (\"X\", \"Y\") VALUES ($1, $2) RETURNING json_build_object('id', \"Id\", 'x', \"X\")",
			args: append(args, "a", 1)},
	}

	for _, v := range buildResults {
		stm, argsOut := v.str.Build()
		assert.Equal(t, v.stm, stm)
		assert.Equal(t, v.args, argsOut)
	}
}

func TestInsertExec(t *testing.T) {
	Init(t)
	ResetTables(t, conn, "test1", "test.\"Test2\"")

	json, err := builder.Insert("test1").Values(insert1).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = builder.Insert("test1").Values(insert1).Returning("id").One(conn, ctx)
	assert.Equal(t, "{\"id\" : 2}", json)
	assert.Equal(t, nil, err)

	jsonMap, err := builder.Insert("test1").Values(insert1).Returning("id").OneMap(conn, ctx)
	assert.True(t, jsonMap["id"].(float64) > 0)
	assert.Equal(t, nil, err)

	jsonMap, err = builder.Insert("test1").Values(insert1).Returning("a_a", "b_B", "cc_cc").OneMap(conn, ctx)
	assert.Equal(t, jsonMap["aA"], "a")
	assert.Equal(t, jsonMap["bB"], float64(1))
	assert.Equal(t, jsonMap["ccCc"], true)
	assert.Equal(t, nil, err)

	jsonMap, err = builder.Insert("test1").Values(insert1).Returning("aA", "bB", "ccCc").OneMap(conn, ctx)
	assert.Equal(t, jsonMap["aA"], "a")
	assert.Equal(t, jsonMap["bB"], float64(1))
	assert.Equal(t, jsonMap["ccCc"], true)
	assert.Equal(t, nil, err)
}
