package pgxjrep_test

import (
	"github.com/divilla/pgxjrep"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"testing"
)

func TestRepositoryExec(t *testing.T) {
	Init(t)
	ResetTables(t, conn, "test1", "test.\"Test2\"")

	repo := pgxjrep.New(builder, conn, ctx)

	//test 1
	json, err := repo.Insert("test.Test2", insert3)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = repo.Insert("test.Test2", insert4)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = repo.Insert("test.Test2", insert3, "id")
	assert.Equal(t, int64(3), gjson.Get(json, "id").Int())
	assert.Equal(t, nil, err)

	json, err = repo.All("test.Test2")
	assert.Equal(t, "[{\"id\":1,\"x\":\"a\",\"y\":1,\"z\":true}, \n {\"id\":2,\"x\":\"c\",\"y\":3,\"z\":true}, \n {\"id\":3,\"x\":\"a\",\"y\":1,\"z\":true}]", json)
	assert.Equal(t, nil, err)

	// test 2
	filter := map[string]interface{}{
		"x": "a",
		"y": nil,
	}
	json, err = repo.Filter("test.Test2", filter, "id", 1, 3)
	assert.Equal(t, "[{\"id\":1,\"x\":\"a\",\"y\":1,\"z\":true}, \n {\"id\":3,\"x\":\"a\",\"y\":1,\"z\":true}]", json)
	assert.Equal(t, nil, err)

	json, err = repo.Filter("test.Test2", filter, "id desc", 2, 1)
	assert.Equal(t, "[{\"id\":1,\"x\":\"a\",\"y\":1,\"z\":true}]", json)
	assert.Equal(t, nil, err)

	json, err = repo.Pages("test.Test2", filter, 1)
	assert.Equal(t, uint64(2), gjson.Get(json, "pages").Uint())
	assert.Equal(t, nil, err)

	// test 3
	updateWhere1 := map[string]interface{}{
		"id": 2,
		"y":  11,
	}
	json, err = repo.Update("test.Test2", updateWhere1)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = repo.All("test.Test2")
	assert.Equal(t, "[{\"id\":1,\"x\":\"a\",\"y\":1,\"z\":true}, \n {\"id\":3,\"x\":\"a\",\"y\":1,\"z\":true}, \n {\"id\":2,\"x\":\"c\",\"y\":11,\"z\":true}]", json)
	assert.Equal(t, nil, err)

	updateWhere2 := map[string]interface{}{
		"id": 3,
		"x":  "s",
	}
	json, err = repo.Update("test.Test2", updateWhere2, "id", "x")
	assert.Equal(t, int64(3), gjson.Get(json, "id").Int())
	assert.Equal(t, "s", gjson.Get(json, "x").String())
	assert.Equal(t, nil, err)

	//test 4
	pk2 := map[string]interface{}{
		"id": 2,
	}
	json, err = repo.Delete("test.Test2", pk2)
	assert.Equal(t, int64(1), gjson.Get(json, "rowsAffected").Int())
	assert.Equal(t, nil, err)

	json, err = repo.All("test.Test2")
	assert.Equal(t, "[{\"id\":1,\"x\":\"a\",\"y\":1,\"z\":true}, \n {\"id\":3,\"x\":\"s\",\"y\":1,\"z\":true}]", json)
	assert.Equal(t, nil, err)

	pk3 := map[string]interface{}{
		"id": 3,
	}
	json, err = repo.Delete("test.Test2", pk3, "id", "y")
	assert.Equal(t, int64(3), gjson.Get(json, "id").Int())
	assert.Equal(t, int64(1), gjson.Get(json, "y").Int())
	assert.Equal(t, nil, err)
}
