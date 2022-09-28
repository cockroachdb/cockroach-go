`crdbpgxv5` is a wrapper around the logic for issuing SQL transactions which
performs retries (as required by CockroachDB) when using
[`github.com/jackc/pgx`](https://github.com/jackc/pgx) in standalone-library
mode. `crdbpgxv5` only supports `pgx/v5`.

Note: use `crdbpgx` for `pgx/v4` support

If you're using pgx just as a driver for the standard `database/sql` package,
use the parent `crdb` package instead.
