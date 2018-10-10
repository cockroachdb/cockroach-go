CRDB
====

`crdb` is a wrapper around the logic for issuing SQL transactions which performs
retries (as required by CockroachDB).

Note that unfortunately there is no generic way of extracting a pg error code;
the library has to recognize driver-dependent error types. We currently support
`github.com/lib/pq` and `github.com/jackc/pgx`.

Note for developers: if you make any changes here (especially if they modify public
APIs), please verify that the code in https://github.com/cockroachdb/examples-go 
still works and update as necessary.
