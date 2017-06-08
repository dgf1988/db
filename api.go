package db

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	//连接池
	db *sql.DB
	//数据库名
	db_name string
)

var (
	//空指针错误
	ErrNilPtr = fmt.Errorf("db: destination pointer is nil")
)

//直接使用标准库的API
func Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.Query(query, args...)
}

func QueryRow(query string, args ...interface{}) *sql.Row {
	return db.QueryRow(query, args...)
}

func Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.Exec(query, args...)
}

//连接
func Open(username, password, hostname string, port int, databasename string) error {
	sqldb, err := sql.Open("mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=true", username, password, hostname, port, databasename))
	if err != nil {
		return err
	}
	if err = sqldb.Ping(); err != nil {
		return err
	}
	db = sqldb
	db_name = databasename
	return nil
}

//Use命令
func Use(databasename string) error {
	_, err := Exec(fmt.Sprintf("use %s", databasename))
	db_name = databasename
	return err
}

//命令
func ShowTables() ([]string, error) {
	rows, err := Query("show tables")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := make([]string, 0)
	for rows.Next() {
		var tablename string
		err = rows.Scan(&tablename)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tablename)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	return tables, nil
}

//类型常量
const (
	TypeInt int = iota
	TypeBigint

	TypeFloat
	TypeDouble
	TypeDecimal

	TypeChar
	TypeVarchar
	TypeText
	TypeMediumText
	TypeLongtext

	TypeDate
	TypeDatetime
	TypeYear
	TypeTime
	TypeTimestamp
)

//解析到常量
func parseDbType(typename string) int {
	switch strings.ToLower(typename) {
	//int64
	case "int":
		return TypeInt
	case "bigint":
		return TypeBigint

	//float64
	case "float":
		return TypeFloat
	case "double":
		return TypeDouble
	case "decimal":
		return TypeDecimal

	//string
	case "char":
		return TypeChar
	case "varchar":
		return TypeVarchar
	case "text":
		return TypeText
	case "mediumtext":
		return TypeMediumText
	case "longtext":
		return TypeLongtext

	//time.Time
	case "date":
		return TypeDate
	case "datetime":
		return TypeDatetime
	case "year":
		return TypeYear
	case "timestamp":
		return TypeTimestamp
	case "time":
		return TypeTime
	}
	panic(fmt.Sprintf("db: parse type name error: %s", typename))
}

//格式化到字符串
func formatDbType(typevalue int) string {
	switch typevalue {
	case TypeInt:
		return "int"
	case TypeBigint:
		return "bigint"
	case TypeFloat:
		return "float"
	case TypeDouble:
		return "double"
	case TypeDecimal:
		return "decimal"
	case TypeChar:
		return "char"
	case TypeVarchar:
		return "varchar"
	case TypeText:
		return "text"
	case TypeMediumText:
		return "mediumtext"
	case TypeLongtext:
		return "longtext"
	case TypeDatetime:
		return "datetime"
	case TypeDate:
		return "date"
	case TypeYear:
		return "year"
	case TypeTime:
		return "time"
	case TypeTimestamp:
		return "timestamp"
	}
	panic(fmt.Sprintf("db: parse type name error: %s", typevalue))
}

//解析数据类型
func parseFieldType(typestr string) (string, int, int) {
	var name = regexp.MustCompile(`\w+`).FindString(typestr)
	var lengthstr = regexp.MustCompile(`\d+`).FindString(typestr)
	var length int
	length, _ = strconv.Atoi(lengthstr)
	var value = parseDbType(name)
	return name, value, length
}

//数据库类型
type FieldType struct {
	Name   string
	Value  int
	Length int
}

//输出Sql
func (t FieldType) ToSql() string {
	switch t.Value {
	case TypeDate, TypeDatetime, TypeYear, TypeTime, TypeTimestamp, TypeText, TypeMediumText, TypeLongtext:
		return t.Name
	}
	return fmt.Sprintf("%s(%d)", t.Name, t.Length)
}

//扫描
func (t *FieldType) Scan(v interface{}) error {
	var str string
	buf, ok := v.([]byte)
	if ok {
		str = string(buf)
	} else {
		str, ok = v.(string)
		if !ok {
			return fmt.Errorf("%T (%v) is not accept type", v, v)
		}
	}
	t.Name, t.Value, t.Length = parseFieldType(str)
	return nil
}

//默认值
type FieldDefault struct {
	Null             bool
	Value            string
	CurrentTimestamp bool
}

func (d FieldDefault) ToSql() string {
	return fmt.Sprintf("DEFAULT %s", d.Value)
}

func (d *FieldDefault) Scan(v interface{}) error {
	if v == nil {
		d.Null = true
		d.Value = "NULL"
	} else {
		d.Null = false
		d.Value = string(v.([]byte))
		if d.Value == "CURRENT_TIMESTAMP" {
			d.CurrentTimestamp = true
		}
	}
	return nil
}

func parseNullable(nullable string) bool {
	return strings.ToUpper(nullable) == "YES"
}

//字段描述
type Field struct {
	Name     string
	FullName string
	Type     FieldType
	Null     bool
	Key      string
	Default  FieldDefault
	Extra    string
	Comment  string
}

func (r Field) ToSql() string {
	var strs = make([]string, 0)
	strs = append(strs, fmt.Sprintf("`%s`", r.Name))
	strs = append(strs, r.Type.ToSql())
	if r.Null {
		strs = append(strs, "NULL", r.Default.ToSql())
	} else {
		strs = append(strs, "NOT NULL")
		if !r.Default.Null {
			strs = append(strs, r.Default.ToSql())
		}
	}
	strs = append(strs, r.Extra)
	return strings.Join(strs, " ")
}

type Table struct {
	DbName      string
	TbName      string
	Fields      []Field
	PrimaryKey  string
	UniqueIndex []string

	Fullname string
	// 预备Sql执行语句
	sqlInsert string

	sqlSelect string

	sqlDelete      string
	sqlUpdate      string
	sqlSelectCount string

	sqlArgMark []string
	Len        int
}

func (t Table) ToSql() string {
	stritems := make([]string, 0)
	stritems = append(stritems, fmt.Sprintf("CREATE TABLE `%s` (", t.TbName))
	colitems := make([]string, 0)
	for i := range t.Fields {
		colitems = append(colitems, "\t"+t.Fields[i].ToSql())
	}
	if t.PrimaryKey != "" {
		colitems = append(colitems, fmt.Sprintf("\tPRIMARY KEY (`%s`)", t.PrimaryKey))
	}
	for i := range t.UniqueIndex {
		colitems = append(colitems, fmt.Sprintf("\tUNIQUE KEY `%s_%d` (`%s`)", t.UniqueIndex[i], i, t.UniqueIndex[i]))
	}
	stritems = append(stritems, strings.Join(colitems, ",\n"), ") ENGINE=InnoDB DEFAULT CHARSET=utf8")
	return strings.Join(stritems, "\n")
}

func GetTable(tablename string) (*Table, error) {
	var query string
	query = `
    SELECT
		COLUMN_NAME, COLUMN_TYPE,
		COLUMN_DEFAULT, IS_NULLABLE,
		COLUMN_KEY,	EXTRA, COLUMN_COMMENT
	FROM
		information_schema.COLUMNS
	WHERE
		TABLE_SCHEMA = ? AND TABLE_NAME = ?
	ORDER BY
		 ORDINAL_POSITION
    `
	var rows *sql.Rows
	var err error
	rows, err = Query(query, db_name, tablename)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var table Table
	table.Fields = make([]Field, 0)
	table.UniqueIndex = make([]string, 0)
	table.sqlArgMark = make([]string, 0)
	table.DbName = db_name
	table.TbName = tablename

	keys := make([]string, 0)

	for rows.Next() {
		var row Field
		var nullable string
		err = rows.Scan(&row.Name, &row.Type, &row.Default, &nullable, &row.Key, &row.Extra, &row.Comment)
		if err != nil {
			return nil, err
		}
		row.Null = parseNullable(nullable)
		row.FullName = fmt.Sprintf("%s.`%s`", table.TbName, row.Name)
		keys = append(keys, row.FullName)
		table.Fields = append(table.Fields, row)
		table.sqlArgMark = append(table.sqlArgMark, "?")
		if row.Key == "PRI" {
			table.PrimaryKey = row.Name
		} else if row.Key == "UNI" {
			table.UniqueIndex = append(table.UniqueIndex, row.Name)
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	table.Len = len(table.Fields)
	if table.Len == 0 {
		return nil, fmt.Errorf("the table (%s) columns no found", tablename)
	}

	table.Fullname = fmt.Sprintf("%s.%s", table.DbName, table.TbName)
	table.sqlInsert = fmt.Sprintf("INSERT INTO %s", table.Fullname)
	table.sqlDelete = fmt.Sprintf("DELETE FROM %s", table.Fullname)
	table.sqlUpdate = fmt.Sprintf("UPDATE %s", table.Fullname)
	strKeys := strings.Join(keys, ",")
	table.sqlSelect = fmt.Sprintf("SELECT %s FROM %s ", strKeys, table.Fullname)
	table.sqlSelectCount = fmt.Sprintf("SELECT COUNT(%s) FROM %s", table.PrimaryKey, table.Fullname)
	return &table, nil
}

// NullTime 可空时间结构体
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

type NullBytes struct {
	Bytes []byte
	Valid bool
}

func (nb *NullBytes) Scan(value interface{}) error {
	nb.Bytes, nb.Valid = value.([]byte)
	return nil
}

func (nb NullBytes) Value() (driver.Value, error) {
	if !nb.Valid {
		return nil, nil
	}
	return nb.Bytes, nil
}

func (t Table) makeScans() []interface{} {
	scans := make([]interface{}, t.Len)
	for i := range t.Fields {
		switch t.Fields[i].Type.Value {
		case TypeInt, TypeBigint:
			scans[i] = new(int64)
		case TypeDate, TypeDatetime, TypeYear, TypeTime, TypeTimestamp:
			scans[i] = new(time.Time)
		case TypeChar, TypeVarchar, TypeText, TypeMediumText, TypeLongtext:
			scans[i] = new(string)
		case TypeFloat, TypeDouble, TypeDecimal:
			scans[i] = new(float64)
		default:
			scans[i] = new([]byte)
		}
	}
	return scans
}

func (t Table) makeNullableScans() []interface{} {
	scans := make([]interface{}, t.Len)
	for i := range t.Fields {
		switch t.Fields[i].Type.Value {
		case TypeInt, TypeBigint:
			scans[i] = new(sql.NullInt64)
		case TypeDate, TypeDatetime, TypeYear, TypeTime, TypeTimestamp:
			scans[i] = new(NullTime)
		case TypeChar, TypeVarchar, TypeText, TypeMediumText, TypeLongtext:
			scans[i] = new(sql.NullString)
		case TypeFloat, TypeDouble, TypeDecimal:
			scans[i] = new(sql.NullFloat64)
		default:
			scans[i] = new(NullBytes)
		}
	}
	return scans
}

func (t Table) makeStructScans(object interface{}) ([]interface{}, error) {
	scans := make([]interface{}, t.Len)
	rv := reflect.ValueOf(object)
	if rv.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("db: the object (%s) is not a pointer", rv.Kind())
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("db: the pointer (%s) can't point to a struct object", rv.Kind())
	}
	if rv.NumField() != t.Len {
		return nil, fmt.Errorf("db: the object field numbers (%d) not equals table column numbers (%d)", rv.NumField(), t.Len)
	}
	for i := range scans {
		scans[i] = rv.Field(i).Addr().Interface()
	}
	return scans, nil
}

func (t Table) parseSlice(scans []interface{}) []interface{} {
	data := make([]interface{}, t.Len)
	for i := range scans {
		data[i] = parseValue(scans[i])
	}
	return data
}

func (t Table) parseMap(scans []interface{}) map[string]interface{} {
	data := make(map[string]interface{})
	for i := range t.Fields {
		data[t.Fields[i].Name] = parseValue(scans[i])
	}
	return data
}

func parseValue(src interface{}) interface{} {
	if s, ok := src.(driver.Valuer); ok {
		src, _ = s.Value()
	}
	if src == nil {
		return nil
	}
	return reflect.Indirect(reflect.ValueOf(src)).Interface()
}

func convertValue(dest interface{}, src interface{}) error {
	if s, ok := src.(driver.Valuer); ok {
		src, _ = s.Value()
	}
	if d, ok := dest.(sql.Scanner); ok {
		return d.Scan(src)
	}
	switch s := src.(type) {
	case *int64:
		return convertValue(dest, *s)
	case *bool:
		return convertValue(dest, *s)
	case *float64:
		return convertValue(dest, *s)
	case *string:
		return convertValue(dest, *s)
	case *time.Time:
		return convertValue(dest, *s)
	case *[]byte:
		return convertValue(dest, *s)
	//int64
	case int64:
		switch d := dest.(type) {
		case *int64:
			if d == nil {
				return ErrNilPtr
			}
			*d = s
			return nil
		case *string:
			if d == nil {
				return ErrNilPtr
			}
			*d = fmt.Sprint(s)
			return nil
		case *bool:
			if d == nil {
				return ErrNilPtr
			}
			if s == 0 {
				*d = false
				return nil
			} else if s == 1 {
				*d = true
				return nil
			}
			return errors.New(fmt.Sprintf("db: the int64(%v) can't convert value to bool.", s))
		case *float64:
			if d == nil {
				return ErrNilPtr
			}
			*d = float64(s)
			return nil
		}
	case float64:
		switch d := dest.(type) {
		case *float64:
			if d == nil {
				return ErrNilPtr
			}
			*d = s
			return nil
		case *string:
			if d == nil {
				return ErrNilPtr
			}
			*d = fmt.Sprint(s)
			return nil
		case *bool:
			if d == nil {
				return ErrNilPtr
			}
			if s == 0.0 {
				*d = false
				return nil
			} else if s == 1.0 {
				*d = true
				return nil
			}
			return errors.New(fmt.Sprintf("db: the float64(%v) can't convert value to bool.", s))
		}
	case bool:
		switch d := dest.(type) {
		case *bool:
			if d == nil {
				return ErrNilPtr
			}
			*d = s
			return nil
		case *string:
			if d == nil {
				return ErrNilPtr
			}
			*d = fmt.Sprint(s)
			return nil
		case *float64:
			if d == nil {
				return ErrNilPtr
			}
			if s {
				*d = 1.0
			} else {
				*d = 0.0
			}
			return nil
		case *int64:
			if d == nil {
				return ErrNilPtr
			}
			if s {
				*d = 1
			} else {
				*d = 0
			}
			return nil
		}
	case string:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return ErrNilPtr
			}
			*d = s
			return nil
		case *int64:
			if d == nil {
				return ErrNilPtr
			}
			value, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			} else {
				*d = value
				return nil
			}
		case *float64:
			if d == nil {
				return ErrNilPtr
			}
			value, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			} else {
				*d = value
				return nil
			}
		case *bool:
			if d == nil {
				return ErrNilPtr
			}
			value, err := strconv.ParseBool(s)
			if err != nil {
				return err
			} else {
				*d = value
				return nil
			}
		case *time.Time:
			if d == nil {
				return ErrNilPtr
			}
			value, err := time.Parse("2006-01-02 15:04:05", s)
			if err != nil {
				value, err = time.Parse("2006-01-02", s)
				if err != nil {
					return err
				}
			}
			*d = value
			return nil
		}
	case []byte:
		switch d := dest.(type) {
		case *[]byte:
			if d == nil {
				return ErrNilPtr
			}
			*d = s
			return nil
		default:
			return convertValue(dest, string(s))
		}
	case time.Time:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return ErrNilPtr
			}
			*d = s.Format("2006-01-02 15:04:05")
			return nil
		case *time.Time:
			if d == nil {
				return ErrNilPtr
			}
			*d = s
			return nil
		}
	}
	return fmt.Errorf("db: convertValue: type error: %T(%v) => %T", src, src, dest)
}

type Row struct {
	*sql.Row
	t *Table
}

func (r *Row) Scan(dest ...interface{}) error {
	scans := r.t.makeNullableScans()
	err := r.Row.Scan(scans...)
	if err != nil {
		return err
	}
	for i := range dest {
		if dest[i] == nil {
			continue
		}
		err = convertValue(dest[i], scans[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Row) Struct(dest interface{}) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("db: the object (%s) is not a pointer", rv.Kind())
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("db: the pointer (%s) is not point to a struct object", rv.Kind())
	}
	if rv.NumField() != r.t.Len {
		return fmt.Errorf("db: the object field numbers (%d) not equals table column numbers (%d)", rv.NumField(), r.t.Len)
	}

	var err error
	var scans = r.t.makeNullableScans()
	if err = r.Row.Scan(scans...); err != nil {
		return err
	}
	for i := range scans {
		if err = convertValue(rv.Field(i).Addr().Interface(), scans[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Row) Slice() ([]interface{}, error) {
	scans := r.t.makeNullableScans()
	err := r.Row.Scan(scans...)
	if err != nil {
		return nil, err
	}
	return r.t.parseSlice(scans), nil
}

func (r *Row) Map() (map[string]interface{}, error) {
	scans := r.t.makeNullableScans()
	err := r.Row.Scan(scans...)
	if err != nil {
		return nil, err
	}
	return r.t.parseMap(scans), nil
}

type Rows struct {
	*sql.Rows
	t     *Table
	scans []interface{}
}

func (rs *Rows) Scan(dest ...interface{}) error {
	err := rs.Rows.Scan(rs.scans...)
	if err != nil {
		return err
	}
	for i := range dest {
		if dest[i] == nil {
			continue
		}
		err = convertValue(dest[i], rs.scans[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *Rows) Struct(dest interface{}) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("db: the object (%s) is not a pointer", rv.Kind())
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("db: the pointer (%s) is not point to a struct object", rv.Kind())
	}
	if rv.NumField() != rs.t.Len {
		return fmt.Errorf("db: the object field numbers (%d) not equals table column numbers (%d)", rv.NumField(), rs.t.Len)
	}

	var err error
	if err = rs.Rows.Scan(rs.scans...); err != nil {
		return err
	}
	for i := range rs.scans {
		if err = convertValue(rv.Field(i).Addr().Interface(), rs.scans[i]); err != nil {
			return err
		}
	}
	return nil
}

func (rs *Rows) Slice() ([]interface{}, error) {
	err := rs.Rows.Scan(rs.scans...)
	if err != nil {
		return nil, err
	}
	return rs.t.parseSlice(rs.scans), nil
}

func (rs *Rows) Map() (map[string]interface{}, error) {
	err := rs.Rows.Scan(rs.scans...)
	if err != nil {
		return nil, err
	}
	return rs.t.parseMap(rs.scans), nil
}

type Setter struct {
	t     *Table
	query string
	args  []interface{}
}

func (s *Setter) Values(values ...interface{}) (int64, error) {
	listkey := make([]string, 0)
	listvalue := make([]interface{}, 0)
	for i := range values {
		if values[i] == nil {
			continue
		}
		listkey = append(listkey, s.t.Fields[i].FullName+"=?")
		listvalue = append(listvalue, values[i])
	}
	strSql := fmt.Sprintf("%s SET %s %s", s.t.sqlUpdate, strings.Join(listkey, ", "), s.query)
	res, err := Exec(strSql, append(listvalue, s.args...)...)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}

// Add 添加数据
func (t Table) Add(values ...interface{}) (int64, error) {
	listcolname := make([]string, 0)
	listParam := make([]interface{}, 0)
	for i := range values {
		if values[i] == nil {
			continue
		}
		listcolname = append(listcolname, t.Fields[i].FullName)
		listParam = append(listParam, values[i])
	}
	res, err := Exec(fmt.Sprintf("%s (%s) VALUES (%s)", t.sqlInsert, strings.Join(listcolname, ", "), strings.Join(t.sqlArgMark[:len(listParam)], ", ")), listParam...)
	if err != nil {
		return -1, err
	}
	return res.LastInsertId()
}

func (t Table) Del(args ...interface{}) (int64, error) {
	listwhere := make([]string, 0)
	listparam := make([]interface{}, 0)
	for i := range args {
		if args[i] == nil {
			continue
		}
		listwhere = append(listwhere, t.Fields[i].FullName+"=?")
		listparam = append(listparam, args[i])
	}

	res, err := Exec(fmt.Sprintf("%s WHERE %s LIMIT 1", t.sqlDelete, strings.Join(listwhere, " AND ")), listparam...)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}

func (t *Table) Get(args ...interface{}) *Row {
	listwhere := make([]string, 0)
	listparam := make([]interface{}, 0)
	for i := range args {
		if args[i] == nil {
			continue
		}
		listwhere = append(listwhere, t.Fields[i].FullName+"=?")
		listparam = append(listparam, args[i])
	}
	strSql := fmt.Sprintf("%s WHERE %s limit 1", t.sqlSelect, strings.Join(listwhere, " AND "))
	return &Row{
		Row: QueryRow(strSql, listparam...), t: t,
	}
}

func (t *Table) GetMany(args ...interface{}) (*Rows, error) {
	listwhere := make([]string, 0)
	listparam := make([]interface{}, 0)
	for i := range args {
		if args[i] == nil {
			continue
		}
		listwhere = append(listwhere, t.Fields[i].FullName+"=?")
		listparam = append(listparam, args[i])
	}
	strSql := fmt.Sprintf("%s WHERE %s", t.sqlSelect, strings.Join(listwhere, " AND "))
	rows, err := Query(strSql, listparam...)
	if err != nil {
		return nil, err
	}
	return &Rows{
		Rows: rows, t: t, scans: t.makeNullableScans(),
	}, nil
}

func (t *Table) Find(args ...interface{}) *Row {
	listwhere := make([]string, 0)
	listparam := make([]interface{}, 0)
	for i := range args {
		if args[i] == nil {
			continue
		}
		listwhere = append(listwhere, t.Fields[i].FullName+"=?")
		listparam = append(listparam, args[i])
	}
	strSql := fmt.Sprintf("%s WHERE %s limit 1", t.sqlSelect, strings.Join(listwhere, " OR "))
	return &Row{
		Row: QueryRow(strSql, listparam...), t: t,
	}
}

func (t *Table) FindMany(args ...interface{}) (*Rows, error) {
	listwhere := make([]string, 0)
	listparam := make([]interface{}, 0)
	for i := range args {
		if args[i] == nil {
			continue
		}
		listwhere = append(listwhere, t.Fields[i].FullName+"=?")
		listparam = append(listparam, args[i])
	}
	strSql := fmt.Sprintf("%s WHERE %s", t.sqlSelect, strings.Join(listwhere, " OR "))
	rows, err := Query(strSql, listparam...)
	if err != nil {
		return nil, err
	}
	return &Rows{
		Rows: rows, t: t, scans: t.makeNullableScans(),
	}, nil
}

func (t *Table) List(take, skip int) (*Rows, error) {
	rows, err := Query(fmt.Sprintf("%s ORDER BY %s limit ?, ?", t.sqlSelect, t.PrimaryKey), skip, take)
	if err != nil {
		return nil, err
	}
	return &Rows{
		Rows: rows, t: t, scans: t.makeNullableScans(),
	}, nil
}

func (t *Table) ListDesc(take, skip int) (*Rows, error) {
	rows, err := Query(fmt.Sprintf("%s ORDER BY %s DESC limit ?, ?", t.sqlSelect, t.PrimaryKey), skip, take)
	if err != nil {
		return nil, err
	}
	return &Rows{
		Rows: rows, t: t, scans: t.makeNullableScans(),
	}, nil
}

func (t *Table) Update(args ...interface{}) *Setter {
	listwhere := make([]string, 0)
	listparam := make([]interface{}, 0)
	for i := range args {
		if args[i] == nil {
			continue
		}
		listwhere = append(listwhere, t.Fields[i].FullName+"=?")
		listparam = append(listparam, args[i])
	}
	query := fmt.Sprintf("WHERE %s limit 1", strings.Join(listwhere, " AND "))
	return &Setter{
		t: t, query: query, args: listparam,
	}
}

func (t *Table) UpdateMany(args ...interface{}) *Setter {
	listwhere := make([]string, 0)
	listparam := make([]interface{}, 0)
	for i := range args {
		if args[i] == nil {
			continue
		}
		listwhere = append(listwhere, t.Fields[i].FullName+"=?")
		listparam = append(listparam, args[i])
	}
	query := fmt.Sprintf("WHERE %s", strings.Join(listwhere, " AND "))
	return &Setter{
		t: t, query: query, args: listparam,
	}
}

func (t Table) Count() (int64, error) {
	var num int64
	if err := QueryRow(t.sqlSelectCount).Scan(&num); err != nil {
		return -1, err
	}
	return num, nil
}

// Count 统计
func (t Table) CountBy(args ...interface{}) (int64, error) {
	var err error
	var keys = make([]string, 0)
	var param = make([]interface{}, 0)
	for i := range args {
		if args[i] == nil {
			continue
		}
		keys = append(keys, t.Fields[i].FullName+"=?")
		param = append(param, args[i])
	}
	var strSql = fmt.Sprintf("%s WHERE %s ", t.sqlSelectCount, strings.Join(keys, " AND "))
	var num int64
	if err = QueryRow(strSql, param...).Scan(&num); err != nil {
		return -1, err
	}
	return num, nil
}

func (t *Table) Query(query string, args ...interface{}) (*Rows, error) {
	strSql := fmt.Sprintf("%s %s", t.sqlSelect, query)
	rows, err := Query(strSql, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{
		Rows: rows, t: t, scans: t.makeNullableScans(),
	}, nil
}

func (t *Table) QueryRow(query string, args ...interface{}) *Row {
	strSql := fmt.Sprintf("%s %s", t.sqlSelect, query)
	return &Row{
		Row: QueryRow(strSql, args...), t: t,
	}
}
