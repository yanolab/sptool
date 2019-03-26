package sptool

import (
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/syucream/spar/src/types"
)

type Table struct {
	Name   string
	Fields []reflect.StructField
}

func (t *Table) New() interface{} {
	return reflect.New(reflect.StructOf(t.Fields)).Interface()
}

func (t *Table) Columns() []string {
	columns := make([]string, 0, len(t.Fields))
	for _, v := range t.Fields {
		columns = append(columns, v.Name)
	}
	return columns
}

func (t *Table) Vals(i interface{}) []interface{} {
	vals := make([]interface{}, 0, len(t.Fields))
	value := reflect.Indirect(reflect.ValueOf(i))
	for _, f := range t.Fields {
		v := value.FieldByName(f.Name)
		vals = append(vals, v.Interface())
	}
	return vals
}

func toType(ctype types.ColumnType, notNull bool) reflect.Type {
	if ctype.IsArray {
		panic(fmt.Sprintf("array type is not supported yet: %v", ctype))
	}

	switch typ := ctype.TypeTag; typ {
	case types.Bool:
		if notNull {
			return reflect.TypeOf(true)
		}
		return reflect.TypeOf(spanner.NullBool{})
	case types.Int64:
		if notNull {
			return reflect.TypeOf(int64(0))
		}
		return reflect.TypeOf(spanner.NullInt64{})
	case types.Float64:
		if notNull {
			return reflect.TypeOf(float64(0))
		}
		return reflect.TypeOf(spanner.NullFloat64{})
	case types.String:
		if notNull {
			return reflect.TypeOf("")
		}
		return reflect.TypeOf(spanner.NullString{})
	case types.Bytes:
		return reflect.TypeOf([]byte{})
	case types.Date:
		if notNull {
			return reflect.TypeOf(civil.Date{})
		}
		return reflect.TypeOf(spanner.NullDate{})
	case types.Timestamp:
		if notNull {
			return reflect.TypeOf(time.Now())
		}
		return reflect.TypeOf(spanner.NullTime{})
	}

	panic(fmt.Sprintf("unknown type: %v", ctype))
}

func newTableStruct(stmt types.CreateTableStatement) *Table {
	fields := make([]reflect.StructField, 0, len(stmt.Columns))
	for _, c := range stmt.Columns {
		fields = append(fields, reflect.StructField{
			Name: c.Name,
			Type: toType(c.Type, c.NotNull),
		})
	}

	return &Table{
		Name:   stmt.TableName,
		Fields: fields,
	}
}
