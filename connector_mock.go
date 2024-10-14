package bunpgconnector

import (
	"database/sql"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type mockDialect struct {
	*pgdialect.Dialect
}

func newDialect(dialect *pgdialect.Dialect) *mockDialect {
	return &mockDialect{dialect}
}

func (r mockDialect) Init(_ *sql.DB) {}

type MockBunConnSet struct {
	Mock sqlmock.Sqlmock
	db   *sql.DB
}

func NewMockBunConnSet() (*MockBunConnSet, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, fmt.Errorf("sqlmock new: %w", err)
	}

	return &MockBunConnSet{
		Mock: mock,
		db:   db,
	}, nil
}

func (r *MockBunConnSet) ReadPool() *bun.DB {
	return r.connect()
}

func (r *MockBunConnSet) WritePool() *bun.DB {
	return r.connect()
}

func (r *MockBunConnSet) connect() *bun.DB {
	return bun.NewDB(r.db, newDialect(pgdialect.New()))
}
