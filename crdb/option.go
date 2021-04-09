package crdb

import (
	"database/sql"
	"time"

	"github.com/jackc/pgx/v4"
)

type Options struct {
	txOptions    *sql.TxOptions
	pgxTxOptions pgx.TxOptions
	timeout      time.Duration
	maxRetries   int
}

func (o *Options) TxOptions() *sql.TxOptions {
	return o.txOptions
}

func (o *Options) PGXTxOptions() pgx.TxOptions {
	return o.pgxTxOptions
}

type Option func(*Options)

func newOptions(opts ...Option) Options {
	o := Options{
		timeout:    timeout,
		maxRetries: maxRetries,
	}

	for _, fn := range opts {
		fn(&o)
	}

	return o
}

func WithTxOptions(txOptions *sql.TxOptions) Option {
	return func(o *Options) {
		o.txOptions = txOptions
	}
}

func WithPGXTxOptions(txOptions pgx.TxOptions) Option {
	return func(o *Options) {
		o.pgxTxOptions = txOptions
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.timeout = timeout
	}
}

func WithMaxRetries(retries int) Option {
	return func(o *Options) {
		o.maxRetries = retries
	}
}
