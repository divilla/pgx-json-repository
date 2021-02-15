package pgxjrep

import (
	"context"
	"encoding/json"
	"github.com/iancoleman/strcase"
	"github.com/sirupsen/logrus"
	"os"
	"regexp"
	"strings"
)

const PublicSchema = "public"

type DbSchema struct {
	ToDbCase   func(input string) string
	ToJsonCase func(input string) string
	colSchema  map[string]map[string][]ColumnSchema
	colMap     map[string]map[string]map[string]bool
	keywords   []string
}

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

type ColumnData struct {
	DbName   string
	JsonName string
	Value    interface{}
	IsString bool
	IsPk     bool
}

type keywordSchema struct {
	Word string `json:"word"`
}

var log *logrus.Logger

func NewSchema(conn PgxConn, ctx context.Context) (*DbSchema, error) {
	log = logrus.New()
	log.Formatter.(*logrus.TextFormatter).ForceColors = true
	log.Formatter.(*logrus.TextFormatter).DisableTimestamp = false
	log.Level = logrus.TraceLevel
	log.Out = os.Stdout

	dbSchema := &DbSchema{
		ToDbCase:   strcase.ToSnake,
		ToJsonCase: strcase.ToLowerCamel,
		colSchema:  make(map[string]map[string][]ColumnSchema),
		colMap:     make(map[string]map[string]map[string]bool),
	}

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
		return nil, err
	}

	var res []ColumnSchema
	err = json.Unmarshal([]byte(*jsn), &res)
	if err != nil {
		return nil, err
	}

	for _, v := range res {
		if _, ok := dbSchema.colSchema[v.SchemaName]; !ok {
			dbSchema.colSchema[v.SchemaName] = make(map[string][]ColumnSchema)
			dbSchema.colMap[v.SchemaName] = make(map[string]map[string]bool)
		}

		if _, ok := dbSchema.colSchema[v.SchemaName][v.RelationName]; !ok {
			dbSchema.colMap[v.SchemaName][v.RelationName] = make(map[string]bool)
		}

		dbSchema.colSchema[v.SchemaName][v.RelationName] = append(dbSchema.colSchema[v.SchemaName][v.RelationName], v)

		dbSchema.colMap[v.SchemaName][v.RelationName][v.ColumnName] = true
		jsonName := dbSchema.ToJsonCase(v.ColumnName)
		if jsonName != v.ColumnName {
			dbSchema.colMap[v.SchemaName][v.RelationName][jsonName] = false
		}
	}

	//keywords
	sql = "SELECT json_agg(t) FROM (SELECT word FROM pg_get_keywords() WHERE catcode != 'U' ORDER BY 1) t"
	jsn = new(string)
	err = conn.QueryRow(ctx, sql).Scan(jsn)
	if err != nil {
		return nil, err
	}

	var ks []keywordSchema
	err = json.Unmarshal([]byte(*jsn), &ks)
	if err != nil {
		return nil, err
	}

	for _, v := range ks {
		dbSchema.keywords = append(dbSchema.keywords, v.Word)
	}

	return dbSchema, nil
}

func (s *DbSchema) ColSchema(relation string) []ColumnSchema {
	sch, rel := s.resolveNames(relation)

	return s.colSchema[sch][rel]
}

func (s *DbSchema) ColMap(relation string) map[string]bool {
	sch, rel := s.resolveNames(relation)

	return s.colMap[sch][rel]
}

func (s *DbSchema) ResolveColumns(relation string, columns []string) []ColumnData {
	colMap := make(map[string]interface{})
	for _, v := range columns {
		colMap[v] = nil
	}

	return s.ResolveColumnMap(relation, colMap)
}

func (s *DbSchema) ResolveColumnMap(relation string, values map[string]interface{}) []ColumnData {
	var colVals []ColumnData
	var isChar = func(term string) bool {
		for _, v := range []string{"varchar", "char", "text"} {
			if term == v {
				return true
			}
		}
		return false
	}

	cols := s.ColSchema(relation)
	for _, col := range cols {
		cd := ColumnData{
			DbName:   col.ColumnName,
			JsonName: s.ToJsonCase(col.ColumnName),
			Value:    nil,
			IsString: isChar(col.DataType),
			IsPk:     col.IsPrimaryKey,
		}

		if val, ok := values[cd.DbName]; ok {
			cd.Value = val
			colVals = append(colVals, cd)
		} else if val, ok = values[cd.JsonName]; ok {
			cd.Value = val
			colVals = append(colVals, cd)
		}
	}

	var unresolvedColumns []string
	colMap := s.ColMap(relation)
	for k := range values {
		if _, ok := colMap[k]; !ok {
			unresolvedColumns = append(unresolvedColumns, k)
		}
	}
	if len(unresolvedColumns) > 0 {
		log.Warningf("Relation %s does not contain columns: "+strings.Join(unresolvedColumns, ", "), relation)
	}

	return colVals
}

func (s *DbSchema) SingleQuote(value string) string {
	return "'" + value + "'"
}

func (s *DbSchema) Quote(value string) string {
	if strings.ContainsAny(value, "\"") {
		return value
	}

	match, _ := regexp.MatchString("[A-Z]+", value)
	if match {
		return "\"" + value + "\""
	}

	for _, v := range s.keywords {
		if v == value {
			return "\"" + value + "\""
		}
	}

	return value
}

func (s *DbSchema) UnQuote(value string) string {
	if !strings.ContainsAny(value, "\"") {
		return value
	}

	return strings.Replace(value, "\"", "", -1)
}

func (s *DbSchema) QuoteRelation(relation string) string {
	sch, rel := s.resolveNames(relation)

	if sch == PublicSchema {
		return s.Quote(rel)
	}

	return s.Quote(sch) + "." + s.Quote(rel)
}

func (s *DbSchema) resolveNames(relation string) (string, string) {
	var sch, rel string
	names := strings.Split(relation, ".")

	namesLen := len(names)
	if namesLen == 2 {
		sch = names[0]
		rel = names[1]
	} else if namesLen == 1 {
		sch = PublicSchema
		rel = names[0]
	} else {
		log.Panicln("invalid relation name: ", relation)
	}

	if _, ok := s.colSchema[sch]; !ok {
		log.Panicln("schema not found: ", sch)
	}
	if _, ok := s.colSchema[sch][rel]; !ok {
		log.Panicln("relation not found: ", sch, rel)
	}

	return sch, rel
}
