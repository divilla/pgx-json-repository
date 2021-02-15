package pgxjrep_test

import (
	"context"
	"github.com/divilla/pgxjrep"
	"github.com/jackc/pgx/v4"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"os"
	"testing"
)

var pk1 = map[string]interface{}{
	"id": 11,
}

var where1 = map[string]interface{}{
	"a_a":   "a",
	"b_B":   1,
	"cc_cc": nil,
}

var where2 = map[string]interface{}{
	"aA":   "a",
	"bB":   1,
	"ccCc": nil,
}

var insert1 = map[string]interface{}{
	"a_a":   "a",
	"b_B":   1,
	"cc_cc": nil,
}

var insert2 = map[string]interface{}{
	"aA":   "a",
	"bB":   1,
	"ccCc": nil,
}

var insert3 = map[string]interface{}{
	"X": "a",
	"Y": 1,
	"Z": nil,
}

var insert4 = map[string]interface{}{
	"x": "c",
	"y": 3,
	"z": nil,
}

var update1 = map[string]interface{}{
	"id":    22,
	"a_a":   "a",
	"b_B":   1,
	"cc_cc": nil,
}

var update2 = map[string]interface{}{
	"id":   22,
	"aA":   "a",
	"bB":   1,
	"ccCc": nil,
}

var update3 = map[string]interface{}{
	"Id": 22,
	"X":  "a",
	"Y":  1,
	"Z":  nil,
}

var update4 = map[string]interface{}{
	"id": 22,
	"x":  "a",
	"y":  1,
	"z":  nil,
}

var (
	args    []interface{}
	conn    *pgx.Conn
	ctx     context.Context
	log     *logrus.Logger
	builder *pgxjrep.Builder
)

func Init(t *testing.T) {
	if log != nil {
		return
	}

	log = logrus.New()
	log.Formatter.(*logrus.TextFormatter).ForceColors = true
	log.Formatter.(*logrus.TextFormatter).DisableTimestamp = false
	log.Level = logrus.TraceLevel
	log.Out = os.Stdout

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

	builder, err = pgxjrep.NewBuilder(conn, ctx)
	if err != nil {
		t.Fatalf("Unable to initialize Builer: %v\n", err)
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
