package bunpgconnector

import (
	"context"
	"database/sql"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/uptrace/bun"
	"go.uber.org/zap"
)

type (
	Transaction interface {
		Tx() bun.IDB

		Commit(
			ctx context.Context,
		) error

		Rollback(
			ctx context.Context,
		)
	}

	TransactionManager interface {
		Begin(
			ctx context.Context,
			tx bun.IDB,
			opts *sql.TxOptions,
		) (Transaction, error)
	}

	BunTransactionManager struct {
		log     *zap.Logger
		connSet BunConnSet
	}

	BunTransaction struct {
		tx       bun.Tx
		log      *zap.Logger
		isNested bool
	}
)

func NewTransactionManager(
	log *zap.Logger,
	connSet BunConnSet,
) BunTransactionManager {
	return BunTransactionManager{
		log:     log,
		connSet: connSet,
	}
}

func (r BunTransactionManager) Begin(
	ctx context.Context,
	tx bun.IDB,
	opts *sql.TxOptions,
) (Transaction, error) {
	var (
		bunTx    bun.Tx
		err      error
		isNested bool
	)

	if tx != nil {
		bunTx, isNested = tx.(bun.Tx)
	}

	if !isNested {
		bunTx, err = r.connSet.WritePool().BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
	}

	return &BunTransaction{
		tx:       bunTx,
		log:      r.log,
		isNested: isNested,
	}, nil
}

func (r BunTransaction) Commit(ctx context.Context) error {
	if r.isNested {
		return nil
	}

	if err := r.tx.Commit(); err != nil {
		r.log.Error("commit failed", zap.String("reqId", middleware.GetReqID(ctx)), zap.Error(err))

		return err
	}

	return nil
}

func (r BunTransaction) Rollback(ctx context.Context) {
	if r.isNested {
		return
	}

	if err := r.tx.Rollback(); err != nil {
		r.log.Error("rollback failed", zap.String("reqId", middleware.GetReqID(ctx)), zap.Error(err))
	}
}

func (r BunTransaction) Tx() bun.IDB {
	return r.tx
}
