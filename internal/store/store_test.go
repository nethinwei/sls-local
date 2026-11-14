package store

import (
	"context"
	"testing"
)

// | where 与 SQL 接受用户写的表达式：DuckDB 不能读宿主机文件、不能改回配置。
func TestNoExternalAccess(t *testing.T) {
	s, err := Open(context.Background(), "", Options{SQLOffset: DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, q := range []string{
		`SELECT * FROM read_csv('/etc/hosts')`,
		`SELECT * FROM read_text('/etc/hosts')`,
		`SET enable_external_access = true`,
		`INSTALL httpfs`,
	} {
		if _, err := s.db.Exec(q); err == nil {
			t.Errorf("%s: want error", q)
		}
	}
}
