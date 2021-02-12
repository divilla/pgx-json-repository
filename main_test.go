package pgxexec_test

import (
	"context"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/joho/godotenv"
	"os"
	"testing"
)

type pk1Str struct {
	Id int `json:"id"`
}

var pk1Inst = pk1Str{
	Id: 1,
}

type pk2Str struct {
	Id int
}

var pk2Inst = pk2Str{
	Id: 1,
}

type test1Str struct {
	A string      `json:"a"`
	B int         `json:"b"`
	C interface{} `json:"c"`
}

var test1Insert = test1Str{
	A: "a",
	B: 1,
	C: nil,
}

var test1Update = test1Str{
	A: "f",
	B: 11,
	C: true,
}

var test1Returning = test1Str{}

type test2Str struct {
	X string
	Y int
	Z interface{}
}

var test2Insert = test2Str{
	X: "a",
	Y: 1,
	Z: nil,
}

var test2Update = test2Str{
	X: "f",
	Y: 11,
	Z: true,
}

var test2Ret = test2Str{}

type test3Str struct {
	Y int
	Z interface{}
}

var test3Inst = test3Str{
	Y: 1,
	Z: nil,
}

type test4Str struct {
	Id int `db:"Id, pk"`
	X  string
	Y  int
	Z  interface{}
}

var test4UpdatePk = test4Str{
	Id: 2,
	X:  "fff",
	Y:  22,
	Z:  true,
}

type testPGTStr struct {
	A pgtype.Text `json:"a"`
	B pgtype.Int4 `json:"b"`
	C pgtype.Bool `json:"c"`
}

var testPGTInst = testPGTStr{
	A: pgtype.Text{String: "a", Status: pgtype.Present},
	B: pgtype.Int4{Int: 1, Status: pgtype.Present},
	C: pgtype.Bool{Status: pgtype.Null},
}

var (
	id   int
	args []interface{}
	conn *pgx.Conn
	ctx  context.Context
)

func DB(t *testing.T) {
	if conn != nil {
		return
	}

	var err error
	err = godotenv.Load()
	if err != nil {
		t.Fatal("Error loading .env file")
	}

	ctx = context.Background()
	conn, err = pgx.Connect(context.Background(), os.Getenv("PGXEXEC_TEST_DSN"))
	if err != nil {
		t.Fatalf("Unable to connection to database: %v\n", err)
	}

	InitDb(t, conn)
}

func ResetTables(t *testing.T, conn *pgx.Conn, tables ...string) {
	for _, table := range tables {
		_, err := conn.Exec(context.Background(), "TRUNCATE TABLE "+table+" RESTART IDENTITY")
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
	}
}

func InitDb(t *testing.T, conn *pgx.Conn) {
	_, err := conn.Exec(context.Background(), `
		create table if not exists test1
		(
			id serial not null
				constraint test1_pk
					primary key,
			a text not null,
			b integer not null,
			c boolean default true not null
		);
	`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	_, err = conn.Exec(context.Background(), `
		create table if not exists test."Test2"
		(
			"Id" serial not null,
			"X" text not null,
			"Y" integer not null,
			"Z" boolean default true not null
		);
	`)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

//func getSourcePath() string {
//	_, filename, _, _ := runtime.Caller(1)
//	return path.Dir(filename)
//}
