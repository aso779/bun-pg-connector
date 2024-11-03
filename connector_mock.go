package bunpgconnector

import (
	"database/sql"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"go.uber.org/zap"
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
	conf PostgresRW
	log  *zap.Logger
}

func NewMockBunConnSet(
	conf PostgresRW,
	log *zap.Logger,
) (*MockBunConnSet, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, fmt.Errorf("sqlmock new: %w", err)
	}

	return &MockBunConnSet{
		Mock: mock,
		db:   db,
		conf: conf,
		log:  log,
	}, nil
}

func (r *MockBunConnSet) ReadPool() *bun.DB {
	return r.connect(r.conf.Read, r.log)
}

func (r *MockBunConnSet) WritePool() *bun.DB {
	return r.connect(r.conf.Write, r.log)
}

func (r *MockBunConnSet) connect(
	conf Postgres,
	log *zap.Logger,
) *bun.DB {
	db := bun.NewDB(r.db, newDialect(pgdialect.New()))

	if conf.Log.IsEnable() {
		db.AddQueryHook(NewLogQueryHook(conf, log))
	}

	return db
}
