package pgxjrep_test

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSchema(t *testing.T) {
	Init(t)

	cols := builder.ColSchema("test1")
	assert.Equal(t, 4, len(cols))

	cols = builder.ColSchema("test.Test2")
	assert.Equal(t, 4, len(cols))

	assert.Equal(t, "\"acaXac\"", builder.Quote("acaXac"))
	assert.Equal(t, "\"cast\"", builder.Quote("cast"))
}
