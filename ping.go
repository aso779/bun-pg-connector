package bunpgconnector

import (
	"fmt"

	"github.com/uptrace/bun"
)

func Ping(pool *bun.DB) error {
	if pool == nil {
		panic("nil connection pool")
	}

	err := pool.Ping()
	if err != nil {
		return fmt.Errorf("db ping: %w", err)
	}

	return nil
}
