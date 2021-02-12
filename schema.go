package pgxexec

import (
	"context"
	"encoding/json"
	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	"strings"
)

type Schemata map[string]TableSchema

type TableSchema map[string][]ColumnSchema

type ColumnSchema struct {
	SchemaName             string   `json:"schemaName"`
	RelationName           string   `json:"relationName"`
	ColumnName             string   `json:"columnName"`
	Position               int16    `json:"position"`
	TypeOid                int64    `json:"typeOid"`
	DataType               string   `json:"dataType"`
	TypeType               string   `json:"typeType"`
	Size                   int      `json:"size"`
	Modifier               int      `json:"modifier"`
	Dimension              int      `json:"dimension"`
	CharacterMaximumLength *int     `json:"characterMaximumLength"`
	NumericPrecision       *int     `json:"numericPrecision"`
	NumericScale           *int     `json:"numericScale"`
	EnumValues             []string `json:"enumValues"`
	DefaultValue           string   `json:"defaultValue"`
	IsNotNull              bool     `json:"isNotNull"`
	IsGenerated            bool     `json:"isGenerated"`
	IsPrimaryKey           bool     `json:"isPrimaryKey"`
	IsRequired             bool     `json:"isRequired"`
	IsReadonly             bool     `json:"isReadonly"`
	ColumnComment          bool     `json:"columnComment"`
}

type KeywordSchema struct {
	Word string `json:"word"`
}

var (
	schemata = make(Schemata)
	keywords []string
)

func InitSchema(conn PgxConn, ctx context.Context) error {
	sql := `
		SELECT json_agg(t)
			FROM (SELECT
				d.nspname::text                                                                                             AS "schemaName",
				c.relname::text                                                                                             AS "relationName",
				a.attname::text                                                                                             AS "columnName",
				a.attnum                                                                                                    AS "position",
				COALESCE(td.oid, tb.oid, t.oid)::bigint                                                                     AS "typeOid",
				format_type(atttypid, NULL::integer)                                                                        AS "dataType",
				COALESCE(td.typtype, tb.typtype, t.typtype)::text                                                           AS "typeType",
				a.attlen::int                                                                                               AS "size",
				a.atttypmod::int                                                                                            AS "modifier",
				COALESCE(NULLIF(a.attndims, 0), NULLIF(t.typndims, 0), (t.typcategory='A')::int)                            AS "dimension",
				CAST(
						 information_schema._pg_char_max_length(information_schema._pg_truetypid(a, t),
						information_schema._pg_truetypmod(a, t)) AS int
				)                                                                                                           AS "characterMaximumLength",
				CASE atttypid
					WHEN 21 /*int2*/ THEN 16
					WHEN 23 /*int4*/ THEN 32
					WHEN 20 /*int8*/ THEN 64
					WHEN 1700 /*numeric*/ THEN
						CASE WHEN atttypmod = -1
							THEN null
							ELSE ((atttypmod - 4) >> 16) & 65535
						END
					WHEN 700 /*float4*/ THEN 24 /*FLT_MANT_DIG*/
					WHEN 701 /*float8*/ THEN 53 /*DBL_MANT_DIG*/
					ELSE null
				END::int                                                                                                    AS "numericPrecision",
				CASE
					WHEN atttypid IN (21, 23, 20) THEN 0
					WHEN atttypid IN (1700) THEN
					CASE
						WHEN atttypmod = -1 THEN null
						ELSE (atttypmod - 4) & 65535
					END
					ELSE null
				END::int                                                                                                    AS "numericScale",
				CASE WHEN COALESCE(td.typtype, tb.typtype, t.typtype) = 'e'::char
					THEN (SELECT array_agg(enumlabel) FROM pg_enum WHERE enumtypid = COALESCE(td.oid, tb.oid, a.atttypid))
					ELSE NULL
				END                                                                                                         AS "enumValues",
				CASE
					WHEN a.attgenerated = '' THEN pg_get_expr(ad.adbin, ad.adrelid)
					ELSE NULL::text
					END::information_schema.character_data                                                                  AS "defaultValue",
				a.attnotnull                                                                                                AS "isNotNull",
				CASE
					WHEN coalesce(pg_get_expr(ad.adbin, ad.adrelid) ~ 'nextval', false)
						OR attidentity != '' OR attgenerated != ''
						OR (t.typname = 'uuid' AND LENGTH(COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '')) > 0) THEN true
					ELSE false
					END::bool                                                                                               AS "isGenerated",
				CASE
					WHEN a.attnum = any (ct.conkey) THEN true
					ELSE false
					END::bool                                                                                               AS "isPrimaryKey",
				CASE
					WHEN length(coalesce(pg_get_expr(ad.adbin, ad.adrelid), '')) > 0
						OR attidentity != '' OR attgenerated != ''
						OR a.attnotnull = false THEN false
					ELSE true
					END::bool                                                                                               AS "isRequired",
				CASE
					WHEN coalesce(pg_get_expr(ad.adbin, ad.adrelid) ~ 'nextval', false)
						OR attidentity != '' OR attgenerated != ''
						OR (t.typname = 'uuid' AND length(coalesce(pg_get_expr(ad.adbin, ad.adrelid), '')) > 0) THEN true
					WHEN (c.relkind = ANY (ARRAY ['r', 'p']))
						OR (c.relkind = ANY (ARRAY ['v', 'f'])) AND pg_column_is_updatable(c.oid::regclass, a.attnum, false) THEN false
					ELSE true
					END::bool                                                                                               AS "isReadonly",
				pg_catalog.col_description(c.oid, a.attnum)                                                                 AS "columnComment"
			FROM
				pg_class c
				LEFT JOIN pg_attribute a ON a.attrelid = c.oid
				LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
				LEFT JOIN pg_type t ON a.atttypid = t.oid
				LEFT JOIN pg_type tb ON (a.attndims > 0 OR t.typcategory='A') AND t.typelem > 0 AND t.typelem = tb.oid OR t.typbasetype > 0 AND t.typbasetype = tb.oid
				LEFT JOIN pg_type td ON t.typndims > 0 AND t.typbasetype > 0 AND tb.typelem = td.oid
				LEFT JOIN pg_namespace d ON d.oid = c.relnamespace
				LEFT JOIN pg_constraint ct ON ct.conrelid = c.oid AND ct.contype = 'p'
			WHERE
				a.attnum > 0 AND t.typname != '' AND NOT a.attisdropped
				AND c.relkind IN ('r', 'p', 'f', 'v', 'm')
				AND d.nspname NOT LIKE 'pg_%' AND d.nspname != 'information_schema'
				AND (pg_has_role(c.relowner, 'USAGE'::text) OR has_table_privilege(quote_ident(d.nspname)||'.'||quote_ident(c.relname), 'SELECT'::text))
			ORDER BY
				d.nspname,
				c.relname,
				a.attnum
		) t
`

	jsn := new(string)
	err := conn.QueryRow(ctx, sql).Scan(jsn)
	if err != nil {
		return err
	}

	var res []ColumnSchema
	err = json.Unmarshal([]byte(*jsn), &res)
	if err != nil {
		return err
	}

	for _, v := range res {
		if _, ok := schemata[v.SchemaName]; !ok {
			schemata[v.SchemaName] = TableSchema{}
		}
		if _, ok := schemata[v.SchemaName][v.RelationName]; !ok {
			schemata[v.SchemaName][v.RelationName] = make([]ColumnSchema, 0)
		}
		schemata[v.SchemaName][v.RelationName] = append(schemata[v.SchemaName][v.RelationName], v)
	}

	//keywords
	sql = "SELECT json_agg(t) FROM (SELECT word FROM pg_get_keywords() WHERE catcode != 'U' ORDER BY 1) t"
	jsn = new(string)
	err = conn.QueryRow(ctx, sql).Scan(jsn)
	if err != nil {
		return err
	}

	var ks []KeywordSchema
	err = json.Unmarshal([]byte(*jsn), &ks)
	if err != nil {
		return err
	}

	for _, v := range ks {
		keywords = append(keywords, v.Word)
	}

	return nil
}

func Schema(relation string) ([]ColumnSchema, error) {
	var sch, rel string
	names := strings.Split(relation, ".")

	namesLen := len(names)
	if namesLen == 2 {
		sch = names[0]
		rel = names[1]
	} else if namesLen == 1 {
		sch = "public"
		rel = names[0]
	} else {
		return nil, errors.New("invalid relation name argument")
	}

	if _, ok := schemata[sch]; !ok {
		return nil, errors.New("schema not found")
	}
	if _, ok := schemata[sch][rel]; !ok {
		return nil, errors.New("relation not found")
	}

	return schemata[sch][rel], nil
}

func ColumnNames(relation string) ([]string, error) {
	css, err := Schema(relation)
	if err != nil {
		return nil, err
	}

	var cols []string
	for _, v := range css {
		cols = append(cols, v.ColumnName)
	}

	return cols, nil
}

func ColumnNamesCamelCase(relation string) ([]string, error) {
	css, err := Schema(relation)
	if err != nil {
		return nil, err
	}

	var cols []string
	for _, v := range css {
		cols = append(cols, strcase.ToLowerCamel(v.ColumnName))
	}

	return cols, nil
}

func PrimaryKey(relation string) ([]string, error) {
	css, err := Schema(relation)
	if err != nil {
		return nil, err
	}

	var cols []string
	for _, v := range css {
		if v.IsPrimaryKey {
			cols = append(cols, v.ColumnName)
		}
	}

	return cols, nil
}
