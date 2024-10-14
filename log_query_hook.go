package bunpgconnector

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/uptrace/bun"
	"go.uber.org/zap"
)

type Option func(*LogQueryHook)

type LogQueryHook struct {
	config Postgres
	log    *zap.Logger
}

var _ bun.QueryHook = (*LogQueryHook)(nil)

func NewLogQueryHook(
	conf Postgres,
	log *zap.Logger,
	opts ...Option,
) *LogQueryHook {
	h := &LogQueryHook{
		config: conf,
		log:    log,
	}
	for _, opt := range opts {
		opt(h)
	}

	return h
}

func (r *LogQueryHook) BeforeQuery(
	ctx context.Context, _ *bun.QueryEvent,
) context.Context {
	return ctx
}

func (r *LogQueryHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	now := time.Now().UTC()
	dur := now.Sub(event.StartTime)

	if errors.Is(event.Err, sql.ErrNoRows) || errors.Is(event.Err, sql.ErrTxDone) {
		r.log.Info("bun",
			zap.String("reqId", middleware.GetReqID(ctx)),
			zap.String("operation", event.Operation()),
			zap.Duration("duration", dur.Round(time.Microsecond)),
			zap.String("query", event.Query),
			zap.Error(event.Err),
		)

		return
	}

	if event.Err != nil {
		r.log.Error("bun",
			zap.String("reqId", middleware.GetReqID(ctx)),
			zap.String("operation", event.Operation()),
			zap.Duration("duration", dur.Round(time.Microsecond)),
			zap.String("query", event.Query),
			zap.Error(event.Err),
		)

		return
	}

	for _, v := range r.config.Log.SkippedQueries() {
		if event.Query == v {
			return
		}
	}

	r.log.Info("bun",
		zap.String("reqId", middleware.GetReqID(ctx)),
		zap.String("operation", event.Operation()),
		zap.Duration("duration", dur.Round(time.Microsecond)),
		zap.String("query", event.Query),
	)
}
