package config

import (
	"testing"
)

func TestParseDSN(t *testing.T) {
	sc := &SourceConfig{}
	sc.MysqlConn = "root:password@tcp(182.234.33.45:3306)/store?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai"
	if err := sc.ParseDSN(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", sc.DBConfig)
	sc = &SourceConfig{}
	sc.MysqlConn = "root:password@tcp(182.234.33.45:3306)/?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai"
	if err := sc.ParseDSN(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", sc.DBConfig)
	sc = &SourceConfig{}
	sc.MysqlConn = "root:password@tcp(182.234.33.45:3306)/s"
	if err := sc.ParseDSN(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", sc.DBConfig)
}
