package pgxexec

import (
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/pkg/errors"
	"log"
	"reflect"
	"strings"
)

const (
	SPDbTagName           = "db"
	SPJsonTagName         = "json"
	SPOmitTagValue        = "-"
	SPOmitEmptyTagOption  = "omitempty"
	SPPrimaryKeyTagOption = "pk"
	SPStartsWithTagOption = "sw"
	SPEndsWithTagOption   = "ew"
	SPContainsTagOption   = "ct"
	SPStringTagOption     = "string"
)

type Struct struct {
	FieldNames []string
	Fields     []FieldValue
	raw        interface{}
	value      *reflect.Value
	typ        reflect.Type
}

type FieldValue struct {
	ColumnName string
	JsonName   string
	As         string
	PrimaryKey bool
	StartsWith bool
	EndsWith   bool
	Contains   bool
	Value      interface{}
}

var NotPointerErr = errors.New("'i' argument must be pointer")

func FieldValues(i interface{}) []FieldValue {
	s := newStruct(i)
	s.parse(false)
	return s.Fields
}

func FieldPointers(i interface{}) []FieldValue {
	if reflect.ValueOf(i).Kind() != reflect.Ptr {
		log.Panic(NotPointerErr)
	}
	s := newStruct(i)
	s.parse(true)
	return s.Fields
}

func FieldNames(i interface{}) []string {
	s := newStruct(i)
	s.parse(false)
	return s.FieldNames
}

func Pointers(fvs []FieldValue) []interface{} {
	var is []interface{}
	for _, v := range fvs {
		is = append(is, v.Value)
	}

	return is
}

func newStruct(i interface{}) *Struct {
	return &Struct{
		raw:   i,
		value: structVal(i),
		typ:   structTyp(i),
	}
}

func structVal(i interface{}) *reflect.Value {
	v := reflect.ValueOf(i)

	if v.Kind() == reflect.Slice {
		return nil
	}

	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		panic("not struct")
	}

	return &v
}

func structTyp(i interface{}) reflect.Type {
	v := reflect.TypeOf(i)

	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Slice {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		panic("not struct")
	}

	return v
}

func (s *Struct) parse(ptrs bool) {
	if s.value == nil {
		for i := 0; i < s.typ.NumField(); i++ {
			field := s.typ.Field(i)
			if field.PkgPath != "" {
				continue
			}

			dbTag := parseTag(field, SPDbTagName)
			jsonTag := parseTag(field, SPJsonTagName)
			if dbTag.value == SPOmitTagValue {
				continue
			}

			name := field.Name
			if dbTag.value != "" {
				name = dbTag.value
			} else if jsonTag.value != "" {
				name = jsonTag.value
			}

			s.FieldNames = append(s.FieldNames, name)
		}

		return
	}

	t := s.value.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}

		dbTag := parseTag(field, SPDbTagName)
		jsonTag := parseTag(field, SPJsonTagName)
		if dbTag.value == SPOmitTagValue {
			continue
		}

		name := field.Name
		if dbTag.value != "" {
			name = dbTag.value
		} else if jsonTag.value != "" {
			name = jsonTag.value
		}

		val := s.value.FieldByName(field.Name)
		finalVal := val.Interface()
		if strings.HasPrefix(val.Type().String(), "pgtype") {
			pgStruct := newStruct(val.Interface())
			status := pgStruct.value.FieldByName("Status")
			if status.Interface() == pgtype.Undefined {
				continue
			}
			if status.Interface() == pgtype.Null {
				finalVal = nil
			}
		}

		if dbTag.contains(SPOmitEmptyTagOption) {
			if finalVal == nil {
				continue
			}

			zero := reflect.Zero(val.Type()).Interface()
			if reflect.DeepEqual(finalVal, zero) {
				continue
			}
		}

		if dbTag.contains(SPStringTagOption) {
			str, ok := val.Interface().(fmt.Stringer)
			if ok {
				finalVal = str.String()
			}
		}

		s.FieldNames = append(s.FieldNames, name)
		if ptrs && val.Type().Kind() != reflect.Ptr {
			s.Fields = append(s.Fields, FieldValue{
				ColumnName: name,
				JsonName:   jsonTag.value,
				PrimaryKey: dbTag.contains(SPPrimaryKeyTagOption),
				StartsWith: dbTag.contains(SPStartsWithTagOption),
				EndsWith:   dbTag.contains(SPEndsWithTagOption),
				Contains:   dbTag.contains(SPContainsTagOption),
				Value:      val.Addr().Interface(),
			})
		} else {
			s.Fields = append(s.Fields, FieldValue{
				ColumnName: name,
				JsonName:   jsonTag.value,
				PrimaryKey: dbTag.contains(SPPrimaryKeyTagOption),
				StartsWith: dbTag.contains(SPStartsWithTagOption),
				EndsWith:   dbTag.contains(SPEndsWithTagOption),
				Contains:   dbTag.contains(SPContainsTagOption),
				Value:      finalVal,
			})
		}
	}
}

type structTag struct {
	value   string
	options []string
}

func parseTag(field reflect.StructField, tag string) *structTag {
	t := field.Tag.Get(tag)
	if t == "" {
		return &structTag{}
	}

	st := &structTag{}
	for k, v := range strings.Split(t, ",") {
		tv := strings.TrimSpace(v)
		if tv == "" {
			continue
		}

		if k == 0 {
			st.value = tv
		} else {
			st.options = append(st.options, tv)
		}
	}

	return st
}

func (st *structTag) contains(opt string) bool {
	for _, v := range st.options {
		if v == opt {
			return true
		}
	}

	return false
}
