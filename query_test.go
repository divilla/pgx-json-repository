package pgxjrep_test

import (
	"github.com/divilla/pgxjrep"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"testing"
)

type queryBuild struct {
	str  *pgxjrep.QueryStatement
	stm  string
	args []interface{}
	err  error
}

func TestQueryBuild(t *testing.T) {
	Init(t)

	buildResults := []queryBuild{
		{str: builder.Query("test1"),
			stm: "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1"},
		{str: builder.Query("test.Test2"),
			stm: "SELECT \"Id\" AS id, \"X\" AS x, \"Y\" AS y, \"Z\" AS z FROM test.\"Test2\""},
		{str: builder.Query("public.test1"),
			stm: "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1"},
		{str: builder.Query("test1").Select("a_a", "b_B", "cc_cc"),
			stm: "SELECT a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1"},
		{str: builder.Query("test1").Select("aA", "bB", "ccCc"),
			stm: "SELECT a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1"},
		{str: builder.Query("test1").Distinct(),
			stm: "SELECT DISTINCT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1"},
		{str: builder.Query("test1").WhereStatement("a = ? AND b = ? AND c = ?", "a", 1, nil),
			stm:  "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1 WHERE a = $1 AND b = $2 AND c = $3",
			args: append(args, "a", 1, nil)},
		{str: builder.Query("test1").WhereStatement("a_1 = ? AND \"B_B\" = ? AND \"1C\" = ?", "a", 1, nil),
			stm:  "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1 WHERE a_1 = $1 AND \"B_B\" = $2 AND \"1C\" = $3",
			args: append(args, "a", 1, nil)},
		{str: builder.Query("test1").Where(pk1),
			stm:  "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1 WHERE id = $1",
			args: append(args, 11)},
		{str: builder.Query("test.Test2").Where(pk1),
			stm:  "SELECT \"Id\" AS id, \"X\" AS x, \"Y\" AS y, \"Z\" AS z FROM test.\"Test2\" WHERE \"Id\" = $1",
			args: append(args, 11)},
		{str: builder.Query("test1").Where(where1),
			stm:  "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1 WHERE a_a LIKE $1 AND \"b_B\" = $2 AND cc_cc IS NULL",
			args: append(args, "a", 1)},
		{str: builder.Query("test1").Where(where2),
			stm:  "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1 WHERE a_a LIKE $1 AND \"b_B\" = $2 AND cc_cc IS NULL",
			args: append(args, "a", 1)},
		{str: builder.Query("test1").Filter(where1),
			stm:  "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1 WHERE a_a ILIKE $1 AND \"b_B\" = $2",
			args: append(args, "a%", 1)},
		{str: builder.Query("test1").Filter(where2),
			stm:  "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1 WHERE a_a ILIKE $1 AND \"b_B\" = $2",
			args: append(args, "a%", 1)},
		{str: builder.Query("test1").OrderBy("a_a, b_b desc"),
			stm: "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1 ORDER BY a_a, b_b DESC"},
		{str: builder.Query("test1").Limit(60).Offset(30),
			stm: "SELECT id, a_a AS \"aA\", \"b_B\" AS \"bB\", cc_cc AS \"ccCc\" FROM test1 LIMIT 60 OFFSET 30"},
	}

	for _, v := range buildResults {
		stm, argsOut := v.str.Build()
		assert.Equal(t, v.stm, stm)
		assert.Equal(t, v.args, argsOut)
	}
}

func TestQueryExec(t *testing.T) {
	Init(t)
	ResetTables(t, conn, "test1", "test.\"Test2\"")

	//test 1
	json, err := builder.Insert("test.Test2").Values(insert3).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = builder.Insert("test.Test2").Values(insert4).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = builder.Insert("test.Test2").Values(insert3).Exec(conn, ctx)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = builder.Query("test.Test2").All(conn, ctx)
	assert.Equal(t, "[{\"id\":1,\"x\":\"a\",\"y\":1,\"z\":true}, \n {\"id\":2,\"x\":\"c\",\"y\":3,\"z\":true}, \n {\"id\":3,\"x\":\"a\",\"y\":1,\"z\":true}]", json)
	assert.Equal(t, nil, err)
}
