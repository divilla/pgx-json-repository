# pgxexec
### 📖 PostgreSQL SQL builder end executor for jackx/pgx driver.

## 🏃‍♀️ Run
```sh
run migrations/create_db.sql as postgres user

run migrations/up.sql as postgres user
```

## 📚 What it does?
- main goal of this module is to simplify building repositories with best Go PosgtreSQL driver on Github: [jaxkx/pgx](https://github.com/jackc/pgx)
- module is built following fluid builder pattern
- module has dedicated reflection functions allowing user to build and execute sql commands with easy
- it's able to both load and scan data from and to instance, creating and executing statements accepting connection interface to simple connection, pooled connection or transaction
- it can return json built with PostgreSQL server, json can be directly sent as controller response, increasing endpoint performance up to 50% on larger datasets
- it's designed to enable pure CQRS with short and clean code

## 📌 Feature
- pgxexec.Insert("test.Test2").Values(test2Insert).Exec(conn, ctx) --- load and insert values from test2Insert
- pgxexec.Update("test.Test2").SetWherePrimaryKey(test4UpdatePk) --- update table with data from test4Update reading primary key (supporting composite pk) from tag
- pgxexec.Query("test.Test2").WhereValues(pk2Inst).OneJson(conn, ctx) --- returns json, ready for response, built on database level enhancing performance up to 20%
- all functionalities are shown in tests
