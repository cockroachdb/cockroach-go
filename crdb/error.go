package crdb

import "fmt"

func newTxnRestartError(
	err error, retryErr error, format string, args ...interface{},
) *TxnRestartError {
	if err == nil || retryErr == nil {
		return nil
	}
	return &TxnRestartError{
		cause:      err,
		RetryCause: retryErr,
		msg:        fmt.Sprintf(format, args...),
	}
}

// TxnRestartError represents an error when restarting a transaction. It includes
// the original error which triggered the restart.
type TxnRestartError struct {
	cause      error
	RetryCause error
	msg        string
}

func (w *TxnRestartError) Error() string { return w.msg + ": " + w.cause.Error() }
func (w *TxnRestartError) Cause() error  { return w.cause }
