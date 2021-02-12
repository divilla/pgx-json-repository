package pgxexec_test

import (
	"github.com/divilla/pgxexec"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

type insertBuild struct {
	str  *pgxexec.InsertStatement
	stm  string
	args []interface{}
	ret  int
	err  error
}

func TestInsertBuild(t *testing.T) {
	buildResults := []insertBuild{
		{str: pgxexec.Insert("test"), stm: "INSERT INTO test DEFAULT VALUES"},
		{str: pgxexec.Insert(""), stm: "", args: nil, err: pgxexec.TargetRequiredErr},
		{str: pgxexec.Insert("Test"), stm: "INSERT INTO \"Test\" DEFAULT VALUES"},
		{str: pgxexec.Insert("public.Test"), stm: "INSERT INTO \"Test\" DEFAULT VALUES"},
		{str: pgxexec.Insert("test.Test"), stm: "INSERT INTO test.\"Test\" DEFAULT VALUES"},
		{str: pgxexec.Insert("Test.Test"), stm: "INSERT INTO \"Test\".\"Test\" DEFAULT VALUES"},
		{str: pgxexec.Insert("test").Values(test1Insert), stm: "INSERT INTO test (a, b) VALUES ($1, $2)", args: append(args, "a", 1)},
		{str: pgxexec.Insert("test.Test2").Values(test2Insert), stm: "INSERT INTO test.\"Test2\" (\"X\", \"Y\") VALUES ($1, $2)", args: append(args, "a", 1)},
		{str: pgxexec.Insert("test").Values(test1Insert).ValueSet("a", "x"),
			stm:  "INSERT INTO test (a, b) VALUES ($1, $2)",
			args: append(args, "x", 1)},
		{str: pgxexec.Insert("test").Values(test1Insert).ValueSet("d", "x"),
			stm:  "INSERT INTO test (a, b, d) VALUES ($1, $2, $3)",
			args: append(args, "a", 1, "x")},
		{str: pgxexec.Insert("test").Values(test1Insert).Returning(&returning1Inst),
			stm:  "INSERT INTO test (a, b) VALUES ($1, $2) RETURNING id, a",
			args: append(args, "a", 1)},
		{str: pgxexec.Insert("test.Test2").Values(test2Insert).Returning(&returning2Inst),
			stm: "INSERT INTO test.\"Test2\" (\"X\", \"Y\") VALUES ($1, $2) RETURNING \"Id\", \"X\"", args: append(args, "a", 1)},
	}

	for _, v := range buildResults {
		stm, argsOut, err := v.str.Build()
		assert.Equal(t, v.stm, stm)
		assert.Equal(t, v.args, argsOut)
		assert.Equal(t, v.err, err)
	}
}

type insertExec struct {
	str *pgxexec.InsertStatement
	ret func() bool
	err error
}

type returning1Struct struct {
	Id int    `json:"id"`
	A  string `json:"a"`
}

var returning1Inst = returning1Struct{}

type returning2Struct struct {
	Id int
	X  string
}

var returning2Inst = returning2Struct{}

func TestInsertExec(t *testing.T) {
	if conn == nil {
		DB(t)
	}

	_, err := pgxexec.Insert("test1").Values(test1Insert).Exec(conn, ctx)
	assert.Equal(t, nil, err)

	execResults := []insertExec{
		{str: pgxexec.Insert("test1").Values(test1Insert).Returning(&returning1Inst),
			ret: func() bool {
				return returning1Inst.Id > 0 && returning1Inst.A == "a"
			}},
		{str: pgxexec.Insert("test.Test2").Values(test2Insert).Returning(&returning2Inst),
			ret: func() bool {
				return returning2Inst.Id > 0 && returning2Inst.X == "a"
			}},
		{str: pgxexec.Insert("test.Test2").Values(test2Insert).ValueSet("X", "c").Returning(&returning2Inst),
			ret: func() bool {
				return returning2Inst.Id > 0 && returning2Inst.X == "c"
			}},
		{str: pgxexec.Insert("test.Test2").ValueSet("X", "c").ValueSet("Y", 11).Returning(&returning2Inst),
			ret: func() bool {
				return returning2Inst.Id > 0 && returning2Inst.X == "c"
			}},
	}

	for _, v := range execResults {
		err := v.str.One(conn, ctx)
		assert.Equal(t, true, v.ret())
		assert.Equal(t, v.err, err)
	}

	res, err := pgxexec.Insert("test1").Values(test1Insert).Returning(&returning1Inst).OneJson(conn, ctx)
	assert.Equal(t, nil, err)
	matched, err := regexp.MatchString("{\"id\" : \\d+, \"a\" : \"a\"}", res)
	assert.True(t, matched)
	assert.Equal(t, nil, err)

	res, err = pgxexec.Insert("test.Test2").Values(test2Insert).Returning(&returning2Inst).OneJson(conn, ctx)
	assert.Equal(t, nil, err)
	matched, err = regexp.MatchString("{\"Id\" : \\d+, \"X\" : \"a\"}", res)
	assert.True(t, matched)
	assert.Equal(t, nil, err)
}
