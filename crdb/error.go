package crdb

import (
	"fmt"
	"time"
)

// errorCause returns the original cause of the error, if possible. An
// error has a proximate cause if it's type is compatible with Go's
// errors.Unwrap() or pkg/errors' Cause(); the original cause is the
// end of the causal chain.
func errorCause(err error) error {
	for err != nil {
		if c, ok := err.(interface{ Cause() error }); ok {
			err = c.Cause()
		} else if c, ok := err.(interface{ Unwrap() error }); ok {
			err = c.Unwrap()
		} else {
			break
		}
	}
	return err
}

type txError struct {
	cause error
}

// Error implements the error interface.
func (e *txError) Error() string { return e.cause.Error() }

// Cause implements the pkg/errors causer interface.
func (e *txError) Cause() error { return e.cause }

// Unwrap implements the go error causer interface.
func (e *txError) Unwrap() error { return e.cause }

// AmbiguousCommitError represents an error that left a transaction in an
// ambiguous state: unclear if it committed or not.
type AmbiguousCommitError struct {
	txError
}

func newAmbiguousCommitError(err error) *AmbiguousCommitError {
	return &AmbiguousCommitError{txError{cause: err}}
}

// MaxRetriesExceededError represents an error caused by retying the transaction
// too many times, without it ever succeeding.
type MaxRetriesExceededError struct {
	txError
	msg string
}

func newMaxRetriesExceededError(err error, maxRetries int) *MaxRetriesExceededError {
	const msgPattern = "retrying txn failed after %d attempts. original error: %s."
	return &MaxRetriesExceededError{
		txError: txError{cause: err},
		msg:     fmt.Sprintf(msgPattern, maxRetries, err),
	}
}

// TimeoutError represents an error caused by the transaction not succeeding
// within a specified duration.
type TimeoutError struct {
	txError
	msg string
}

func newTimeoutError(err error, duration time.Duration) *TimeoutError {
	const msgPattern = "txn timed out after %v. original error: %s."
	return &TimeoutError{
		txError: txError{cause: err},
		msg:     fmt.Sprintf(msgPattern, duration, err),
	}
}

// TxnRestartError represents an error when restarting a transaction. `cause` is
// the error from restarting the txn and `retryCause` is the original error which
// triggered the restart.
type TxnRestartError struct {
	txError
	retryCause error
	msg        string
}

func newTxnRestartError(err error, retryErr error) *TxnRestartError {
	const msgPattern = "restarting txn failed. ROLLBACK TO SAVEPOINT " +
		"encountered error: %s. Original error: %s."
	return &TxnRestartError{
		txError:    txError{cause: err},
		retryCause: retryErr,
		msg:        fmt.Sprintf(msgPattern, err, retryErr),
	}
}

// Error implements the error interface.
func (e *TxnRestartError) Error() string { return e.msg }

// RetryCause returns the error that caused the transaction to be restarted.
func (e *TxnRestartError) RetryCause() error { return e.retryCause }
