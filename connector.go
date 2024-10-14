package bunpgconnector

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"os"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"go.uber.org/zap"
)

type BunConnSet interface {
	ReadPool() *bun.DB
	WritePool() *bun.DB
}

type PgBunConnSet struct {
	conf  PostgresRW
	log   *zap.Logger
	read  *bun.DB
	write *bun.DB
}

func NewPgBunConnSet(
	conf PostgresRW,
	log *zap.Logger,
) *PgBunConnSet {
	log.Info("new connection set constructed")

	return &PgBunConnSet{
		conf: conf,
		log:  log,
	}
}

func (r *PgBunConnSet) ReadPool() *bun.DB {
	for readConnRetries := 0; ; readConnRetries++ {
		if r.read == nil {
			r.read = connect(r.conf.Read, r.log)
		}

		if readConnRetries > r.conf.Read.MaxConnRetries() {
			panic("can't connect to read db")
		}

		if err := Ping(r.read); err != nil {
			r.log.Info("connset: ping read conn", zap.Int("attempt", readConnRetries), zap.Error(err))
			r.read = nil

			time.Sleep(r.conf.Read.RetryInterval() * time.Millisecond)
		} else {
			break
		}
	}

	return r.read
}

func (r *PgBunConnSet) WritePool() *bun.DB {
	for writeConnAttempts := 0; ; writeConnAttempts++ {
		if r.write == nil {
			r.write = connect(r.conf.Write, r.log)
		}

		if writeConnAttempts > r.conf.Write.MaxConnRetries() {
			panic("can't connect to write db")
		}

		if err := Ping(r.write); err != nil {
			r.log.Info("connset: ping write conn", zap.Int("attempt", writeConnAttempts), zap.Error(err))
			r.write = nil

			time.Sleep(r.conf.Write.RetryInterval() * time.Millisecond)
		} else {
			break
		}
	}

	return r.write
}

func connect(
	conf Postgres,
	log *zap.Logger,
) *bun.DB {
	var (
		tlsConfig      *tls.Config
		isWithInsecure pgdriver.Option
	)

	if conf.TLS.IsInsecureSkipVerify() {
		tlsConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
		isWithInsecure = pgdriver.WithInsecure(true)
	} else {
		if conf.TLS.IsWithCerts() {
			clientCert, err := tls.LoadX509KeyPair(conf.TLS.ClientCert(), conf.TLS.ClientKey())
			if err != nil {
				log.Error("failed to load client certificate", zap.Error(err))
			}

			caCert, err := os.ReadFile(conf.TLS.ServerCA())
			if err != nil {
				log.Error("failed to read CA certificate: %v", zap.Error(err))
			}

			caCertPool := x509.NewCertPool()
			if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
				log.Error("failed to append CA certificate to pool")
			}

			tlsConfig = &tls.Config{
				MinVersion:         tls.VersionTLS12,
				Certificates:       []tls.Certificate{clientCert},
				RootCAs:            caCertPool,
				InsecureSkipVerify: false,
			}
		}

		isWithInsecure = pgdriver.WithInsecure(false)
	}

	conn := pgdriver.NewConnector(
		pgdriver.WithNetwork("tcp"),
		pgdriver.WithAddr(conf.Addr()),
		pgdriver.WithTLSConfig(tlsConfig),
		pgdriver.WithUser(conf.User()),
		pgdriver.WithPassword(conf.Password()),
		pgdriver.WithDatabase(conf.Database()),
		pgdriver.WithApplicationName(conf.AppName()),
		pgdriver.WithDialTimeout(conf.DialTimeout()*time.Second),
		pgdriver.WithReadTimeout(conf.ReadTimeout()*time.Second),
		pgdriver.WithWriteTimeout(conf.WriteTimeout()*time.Second),
		isWithInsecure,
	)

	db := bun.NewDB(sql.OpenDB(conn), pgdialect.New())

	if conf.Log.IsEnable() {
		db.AddQueryHook(NewLogQueryHook(conf, log))
	}

	return db
}
