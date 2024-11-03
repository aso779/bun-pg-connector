package bunpgconnector

import (
	"fmt"
	"time"

	"github.com/aso779/config-loader"
)

type PostgresRW struct {
	Read  Postgres `yaml:"read"`
	Write Postgres `yaml:"write"`
}

type Postgres struct {
	AppNameProp                 string      `yaml:"app_name"`
	HostProp                    string      `yaml:"host"`
	PortProp                    string      `yaml:"port"`
	UserProp                    string      `yaml:"user"`
	PasswordProp                string      `yaml:"password"`
	DatabaseProp                string      `yaml:"database"`
	MaxOpenConnsProp            string      `yaml:"max_open_conns"`
	MaxIdleConnsProp            string      `yaml:"max_idle_conns"`
	TimezoneProp                string      `yaml:"timezone"`
	DialTimeoutProp             string      `yaml:"dial_timeout"`
	ReadTimeoutProp             string      `yaml:"read_timeout"`
	WriteTimeoutProp            string      `yaml:"write_timeout"`
	MaxConnRetriesProp          string      `yaml:"max_conn_retries"`
	RetryIntervalProp           string      `yaml:"retry_interval"`
	IsDiscardUnknownColumnsProp string      `yaml:"is_discard_unknown_columns"`
	Log                         PostgresLog `yaml:"log"`
	TLS                         TLS         `yaml:"tls"`
}

func (r Postgres) AppName() string {
	return cfgloader.LoadStringProp(r.AppNameProp)
}

func (r Postgres) Host() string {
	return cfgloader.LoadStringProp(r.HostProp)
}

func (r Postgres) Port() int {
	return cfgloader.LoadIntProp(r.PortProp)
}

func (r Postgres) User() string {
	return cfgloader.LoadStringProp(r.UserProp)
}

func (r Postgres) Password() string {
	return cfgloader.LoadStringProp(r.PasswordProp)
}

func (r Postgres) Database() string {
	return cfgloader.LoadStringProp(r.DatabaseProp)
}

func (r Postgres) MaxOpenConns() int {
	return cfgloader.LoadIntProp(r.MaxOpenConnsProp)
}

func (r Postgres) MaxIdleConns() int {
	return cfgloader.LoadIntProp(r.MaxIdleConnsProp)
}

func (r Postgres) Timezone() string {
	return cfgloader.LoadStringProp(r.TimezoneProp)
}

func (r Postgres) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host(), r.Port())
}

func (r Postgres) DialTimeout() time.Duration {
	return cfgloader.LoadDurationProp(r.DialTimeoutProp)
}

func (r Postgres) ReadTimeout() time.Duration {
	return cfgloader.LoadDurationProp(r.ReadTimeoutProp)
}

func (r Postgres) WriteTimeout() time.Duration {
	return cfgloader.LoadDurationProp(r.WriteTimeoutProp)
}

func (r Postgres) MaxConnRetries() int {
	return cfgloader.LoadIntProp(r.MaxConnRetriesProp)
}

func (r Postgres) RetryInterval() time.Duration {
	return cfgloader.LoadDurationProp(r.RetryIntervalProp)
}

func (r Postgres) IsDiscardUnknownColumns() bool {
	return cfgloader.LoadBoolProp(r.IsDiscardUnknownColumnsProp)
}

func (r Postgres) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s timezone=%s",
		r.Host(),
		r.Port(),
		r.User(),
		r.Password(),
		r.Database(),
		func() string {
			if r.TLS.IsInsecureSkipVerify() {
				return "disable"
			}

			return "require"
		}(),
		r.Timezone(),
	)
}

type PostgresLog struct {
	IsEnableProp    string   `yaml:"is_enable"`
	SkipQueriesProp []string `yaml:"skip_queries"`
}

func (r PostgresLog) IsEnable() bool {
	return cfgloader.LoadBoolProp(r.IsEnableProp)
}

func (r PostgresLog) SkippedQueries() []string {
	return r.SkipQueriesProp
}

type TLS struct {
	IsInsecureSkipVerifyProp string `yaml:"is_insecure_skip_verify"`
	ClientCertProp           string `yaml:"client_cert"`
	ClientKeyProp            string `yaml:"client_key"`
	ServerCAProp             string `yaml:"server_ca"`
}

func (r TLS) IsInsecureSkipVerify() bool {
	return cfgloader.LoadBoolProp(r.IsInsecureSkipVerifyProp)
}

func (r TLS) ClientCert() string {
	return cfgloader.LoadStringProp(r.ClientCertProp)
}

func (r TLS) ClientKey() string {
	return cfgloader.LoadStringProp(r.ClientKeyProp)
}

func (r TLS) ServerCA() string {
	return cfgloader.LoadStringProp(r.ServerCAProp)
}

func (r TLS) IsWithCerts() bool {
	if r.ClientCert() != "" && r.ClientKey() != "" && r.ServerCA() != "" {
		return true
	}

	return false
}
