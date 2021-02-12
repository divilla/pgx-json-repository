package pgxexec_test

import (
	"github.com/divilla/pgxexec"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSchema(t *testing.T) {
	if conn == nil {
		DB(t)
	}

	err := pgxexec.InitSchema(conn, ctx)
	if err != nil {
		panic(err)
	}

	cols, err := pgxexec.Schema("test1")
	assert.Equal(t, 4, len(cols))

	cols, err = pgxexec.Schema("test.Test2")
	assert.Equal(t, 4, len(cols))

	assert.Equal(t, "\"acaXac\"", pgxexec.Quote("acaXac"))
	assert.Equal(t, "\"cast\"", pgxexec.Quote("cast"))
}
