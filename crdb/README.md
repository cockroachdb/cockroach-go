CRDB
====

`crdb` is a wrapper around the logic for issuing SQL transactions which performs
retries (as required by CockroachDB).

## Basic Usage

```go
import "github.com/cockroachdb/cockroach-go/v2/crdb"

err := crdb.ExecuteTx(ctx, db, nil, func(tx *sql.Tx) error {
    // Your transaction logic here
    _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    return err
})
```

## Retry Policies

By default, transactions retry up to 50 times with no delay between attempts.
You can customize retry behavior using context options.

### Limiting Retries

```go
// Retry up to 10 times
ctx := crdb.WithMaxRetries(context.Background(), 10)
err := crdb.ExecuteTx(ctx, db, nil, func(tx *sql.Tx) error {
    // ...
})
```

### Unlimited Retries

```go
// Retry indefinitely (use with caution - ensure you have a context timeout!)
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
ctx = crdb.WithMaxRetries(ctx, 0)
```

### Disabling Retries

```go
// Execute only once, no retries
ctx := crdb.WithNoRetries(context.Background())
```

### Fixed Delay Between Retries

```go
ctx := crdb.WithRetryPolicy(context.Background(), &crdb.LimitBackoffRetryPolicy{
    RetryLimit: 10,
    Delay:      100 * time.Millisecond,
})
```

### Exponential Backoff

```go
ctx := crdb.WithRetryPolicy(context.Background(), &crdb.ExpBackoffRetryPolicy{
    RetryLimit: 10,
    BaseDelay:  100 * time.Millisecond,  // First retry waits 100ms
    MaxDelay:   5 * time.Second,          // Cap delay at 5s
})
// Delays: 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, 5s, 5s, 5s, 5s
```

### Custom Retry Policies

Implement the `RetryPolicy` interface for custom behavior:

```go
type RetryPolicy interface {
    NewRetry() RetryFunc
}

type RetryFunc func(err error) (delay time.Duration, retryErr error)
```

You can also adapt third-party backoff libraries using `ExternalBackoffPolicy()`:

```go
import "github.com/sethvargo/go-retry"

ctx := crdb.WithRetryPolicy(context.Background(), crdb.ExternalBackoffPolicy(func() crdb.ExternalBackoff {
    return retry.NewFibonacci(1 * time.Second)
}))
```

## Framework Support

Subpackages provide support for popular frameworks:

| Package | Framework | Import |
|---------|-----------|--------|
| `crdbpgx` | pgx v4 (standalone) | `github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx` |
| `crdbpgxv5` | pgx v5 (standalone) | `github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5` |
| `crdbgorm` | GORM | `github.com/cockroachdb/cockroach-go/v2/crdb/crdbgorm` |
| `crdbsqlx` | sqlx | `github.com/cockroachdb/cockroach-go/v2/crdb/crdbsqlx` |

## Error Wrapping

When wrapping errors inside transaction functions, use `%w` or `errors.Wrap()`
to preserve retry detection:

```go
// WRONG - masks retryable error
return fmt.Errorf("failed: %s", err)

// CORRECT - preserves error for retry detection
return fmt.Errorf("failed: %w", err)
```

## Driver Compatibility

The library detects retryable errors using the `SQLState() string` method,
which is implemented by:
- [`github.com/lib/pq`](https://github.com/lib/pq) (v1.10.6+)
- [`github.com/jackc/pgx`](https://github.com/jackc/pgx) (database/sql driver mode)

## Note for Developers

If you make any changes here (especially if they modify public APIs), please
verify that the code in https://github.com/cockroachdb/examples-go still works
and update as necessary.
