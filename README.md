# pgxjrep
### 📖 PostgreSQL generic json repository and SQL builder for jackx/pgx driver.

## 🏃‍♀️ Run
```sh
run migrations/create_db.sql as postgres user

run migrations/up.sql as pgxjrep user
```

## 📚 What it does?
- loads full database schema, generating 100% correct and executable statements
- full support for upper-cased schemas, relations and columns with automatic quoting
- exec panics on invalid shema.relation name and logs warning on invalid column names
- build SQL Statements with maps unmarshaled directly from json request with automatic camel-cased column recognition - no need for dto structs, db and json tags 
- json result is built on PostgreSQL Server with zero Go marshaling
- generic json repository with common commands provides short and clean code

## 📌 Example repository
```go
package main

import (
	"context"
	"github.com/divilla/pgxjrep"
	"github.com/jackc/pgx/v4"
)

func main()  {
	conn, _ := pgx.Connect(context.Background(), "connection-string")

	//builder supports concurrency you only need to define it once for entire project
	builder, _ := pgxjrep.NewBuilder(conn, context.Background())

	//interface accepts pgx connection, pooled connection or transaction
	repo := pgxjrep.New(builder, conn, context.Background())

	//select all records from relation, Schema.Table will be properly quoted, 
	//you can omit schema_name on public schema
	json, err := repo.All("schema_name.table_name")

	//filter all records from relation_name where 
	//"first_name" starting with "a", "last_name" is ignored as nil, "active" is true,
	//order by "id DESC" and select records from 61 - 90
	json, err := repo.Filter("relation_name",
		map[string]interface{}{ "firstName": "a", "lastName": nil, "active": true },
		"id desc", 3, 30)

	//get total number of pages by filter and pageSize
	json, err := repo.Pages("relation_name",
		map[string]interface{}{ "firstName": "a", "lastName": nil, "active": true },
		30)

	//select by primary key -> composite primary key is fully supported
	pk := map[string]interface{}{
		"id": 1,
	}
	json, err = repo.OneByPk("table", pk)

	//insert
	insert := map[string]interface{}{
		"firstName": "First", //col name can be first_name or "firstName" or "First_Name"
		"lastName": "Last",
	}
	//returns string {"rowsAffected": 1} by default
	json, err = repo.Insert("table", insert)
	//returns string {"id":1}
	json, err = repo.Insert("table", insert, "id")

	//update will auto recognize "id" as primary key and create where condition updating by primary key
	update := map[string]interface{}{
		"id": 1, // you can have "Id" in the database
		"firstName": "First",
		"lastName": "Last",
	}
	//returns string {"rowsAffected": 1} by default
	json, err = repo.Update("table", update)
	//returns string {"id":1}
	json, err = repo.Update("table", update, "id")

	//delete returns {"rowsAffected": 1} by default
	json, err = repo.Delete("table", pk)
	//returns string {"id": 1}
	json, err = repo.Delete("table", update, "id")
}
```

## 📌 Example query
```go
package main

import (
	"context"
	"github.com/divilla/pgxjrep"
	"github.com/jackc/pgx/v4"
)

func main()  {
	conn, _ := pgx.Connect(context.Background(), "connection-string")
	ctx := context.Background()

	//builder supports concurrency you only need to define it once for entire project
	builder, _ := pgxjrep.NewBuilder(conn, ctx)

	//select all records from relation, Schema.Table will be properly quoted, 
	//you can omit schema_name on public schema
	json, err := builder.Query("schema_name.table_name").All(conn, ctx)

	//select * from relation where "first_name" = "a" and "last_name" is null and "active" = true
	json, err := builder.Query("schema_name.table_name").
		Where(map[string]interface{}{ "firstName": "a", "lastName": nil, "active": true }).
		All(conn, ctx)

	//filter all records from relation_name where "first_name" starting with "a", 
	//"last_name" is ignored, "active" is true, 
	//order by "id DESC" and select records from 61 - 90
	json, err := builder.Query("relation_name").
		Filter(map[string]interface{}{ "firstName": "a", "lastName": nil, "active": true }).
		OrderBy("id desc").
		Offset(60).
		Limit(30).
		All(conn, ctx)

	//select single record by primary key -> composite primary key is fully supported
	json, err = builder.Query("relation_name").
		Where(map[string]interface{}{ "id": 1 }).
		One(conn, ctx)

	//get total number of records (uint64) with applied filter
	cnt, err := builder.Query("relation_name").
		Filter(map[string]interface{}{ "firstName": "a", "lastName": nil, "active": true }).
		Count(conn, ctx)

	//get exists (bool) true if there exists a single record
	//where "first_name" = "a" and "last_name" is null and "active" = true
	exists, err := builder.Query("relation_name").
		Where(map[string]interface{}{ "firstName": "a", "lastName": nil, "active": true }).
		Exists(conn, ctx)
}
```

## 📌 Example commands
```go
package main

import (
	"context"
	"github.com/divilla/pgxjrep"
	"github.com/jackc/pgx/v4"
)

func main()  {
	conn, _ := pgx.Connect(context.Background(), "connection-string")
	ctx := context.Background()

	//builder supports concurrency you only need to define it once for entire project
	builder, _ := pgxjrep.NewBuilder(conn, ctx)

	pk := map[string]interface{}{
		"id": 1,
	}

	//insert
	values := map[string]interface{}{
		"firstName": "First", //col name can be first_name or "firstName" or "First_Name"
		"lastName": "Last",
	}
	//returns string {"rowsAffected": 1} by default
	json, err = builder.Insert("table").Values(values).Exec(conn, ctx)
	//returns string {"id":1}
	json, err = builder.Insert("table").Values(values).Returning("id").One(conn, ctx)
	//returns map[string]interface{}{ "id": 1 }
	json, err = builder.Insert("table").Values(values).Returning("id").OneMap(conn, ctx)

	//returns string {"rowsAffected": 1} by default
	json, err = builder.Update("table").Set(values).Where(pk).Exec(conn, ctx)
	//update will auto recognize "id" as primary key and create where condition updating by primary key
	update := map[string]interface{}{
		"id": 1, // you can have "Id" in the database
		"firstName": "First",
		"lastName": "Last",
	}
	//returns string {"id":1}
	json, err = builder.Update("table").SetWherePk(update).Returning("id").One(conn, ctx)
	//returns map[string]interface{}{ "id": 1 }
	json, err = builder.Update("table").SetWherePk(update).Returning("id").OneMap(conn, ctx)

	//delete returns {"rowsAffected": 1} by default
	json, err = builder.Delete("table").Where(pk).Exec(conn, ctx)
	//returns string {"id": 1}
	json, err = builder.Delete("table").Where(pk).Returning("id").One(conn, ctx)
	//returns map[string]interface{}{ "id": 1 }
	json, err = builder.Delete("table").Where(pk).Returning("id").OneMap(conn, ctx)
}
```
