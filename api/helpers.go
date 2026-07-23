package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	chi "github.com/go-chi/chi/v5"
	"github.com/sanskarpan/TransactionManager/api/apiwire"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// errResponse writes a JSON error response. The msg string MUST be a
// sanitized, public-safe message (H-06) — never pass a raw err.Error()
// here. Use errToPublicResponse to derive a safe message + status from
// an internal error.
func errResponse(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// okJSON writes a 200 JSON response.
func okJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// errToPublicResponse maps an internal error to (HTTP status, public-safe
// message). Internal implementation details (txn IDs, raw error strings,
// stack traces) are NOT leaked to the caller. The caller is expected to
// log the raw error with the request ID before calling this function.
func errToPublicResponse(err error) (int, string) {
	var txnErr *types.TxnError
	if errors.As(err, &txnErr) {
		switch txnErr.Code {
		case types.ErrTxnNotFound:
			return http.StatusNotFound, "transaction not found"
		case types.ErrRowNotFound:
			return http.StatusNotFound, "row not found"
		case types.ErrTableNotFound:
			return http.StatusNotFound, "table not found"
		case types.ErrSavepointNotFound:
			return http.StatusNotFound, "savepoint not found"
		case types.ErrDeadlock:
			return http.StatusConflict, "deadlock detected; transaction was aborted and can be retried"
		case types.ErrWriteConflict:
			return http.StatusConflict, "write-write conflict; transaction can be retried"
		case types.ErrSerializationFailure:
			return http.StatusConflict, "serialization failure; transaction can be retried"
		case types.ErrDuplicateKey:
			return http.StatusConflict, "duplicate key"
		case types.ErrLockTimeout:
			return http.StatusRequestTimeout, "lock acquisition timed out"
		case types.ErrTxnAborted:
			return http.StatusGone, "transaction was aborted"
		case types.ErrWounded:
			return http.StatusConflict, "transaction was wounded by a higher-priority transaction"
		case types.ErrInvalidIsolation:
			return http.StatusBadRequest, "unknown isolation level"
		case types.ErrInvalidRowShape:
			return http.StatusBadRequest, "row shape does not match table declaration"
		case types.ErrTxnCommitted:
			return http.StatusConflict, "transaction already committed"
		}
	}
	if errors.Is(err, io.EOF) {
		return http.StatusBadRequest, "request body required"
	}
	if errors.Is(err, http.ErrHandlerTimeout) {
		return http.StatusServiceUnavailable, "request timed out"
	}
	return http.StatusInternalServerError, "internal server error"
}

// errWrite writes a sanitized error response and returns the matched
// status code so the caller can record metrics.
func errWrite(w http.ResponseWriter, err error) (status int) {
	status, msg := errToPublicResponse(err)
	errResponse(w, status, msg)
	return status
}

// decodeJSON decodes a JSON request body with consistent error handling.
// Returns the HTTP status code the caller should write:
//   - 0  → success (req is populated)
//   - 400 → malformed JSON or empty body (caller should write "invalid
//     request body" or "request body required" depending on allowEmpty)
//   - 413 → body exceeded MaxRequestBodyBytes (caller should write 413)
//
// allowEmpty=true treats io.EOF as success (used by /api/txn/begin, which
// accepts an empty body to default to 2PL/ReadCommitted).
func decodeJSON(r *http.Request, dst interface{}, allowEmpty bool) (status int, publicMsg string) {
	err := json.NewDecoder(r.Body).Decode(dst)
	if err == nil {
		return 0, ""
	}
	if errors.Is(err, io.EOF) {
		if allowEmpty {
			return 0, ""
		}
		return http.StatusBadRequest, "request body required"
	}
	// MaxBytesReader wraps the error so http.MaxBytesError is detectable
	// via errors.As; emit 413 rather than a generic 400.
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		return http.StatusRequestEntityTooLarge, "request body too large"
	}
	return http.StatusBadRequest, "invalid request body"
}

// parseTxnID extracts and validates the {id} URL parameter.
// M-35: id == 0 is the reserved seed transaction ID and is rejected so a
// caller cannot probe/manipulate internal seed state.
func parseTxnID(r *http.Request) (txn.ID, error) {
	raw := chi.URLParam(r, "id")
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, errors.New("invalid transaction id")
	}
	if n == 0 {
		return 0, errors.New("invalid transaction id")
	}
	return txn.ID(n), nil
}

// getChiParam was a helper to avoid importing chi directly; no caller
// remains, so it has been removed.

// isolationFromString converts a string name to IsolationLevel.
// Returns ErrInvalidIsolation (so the caller can 400) for unknown values
// — C-08: previously returned a generic errors.New, which the caller
// silently discarded.
func isolationFromString(s string) (txn.IsolationLevel, error) {
	switch s {
	case "read_uncommitted", "ru":
		return txn.ReadUncommitted, nil
	case "read_committed", "rc", "":
		return txn.ReadCommitted, nil
	case "repeatable_read", "rr":
		return txn.RepeatableRead, nil
	case "serializable", "s":
		return txn.Serializable, nil
	}
	return 0, &types.TxnError{Code: types.ErrInvalidIsolation, Message: "unknown isolation level: " + s}
}

// protocolFromString converts a string name to ConcurrencyProtocol.
func protocolFromString(s string) (txn.ConcurrencyProtocol, error) {
	switch s {
	case "2pl", "":
		return txn.Protocol2PL, nil
	case "mvcc":
		return txn.ProtocolMVCC, nil
	}
	return 0, &types.TxnError{Code: types.ErrInvalidIsolation, Message: "unknown protocol: " + s}
}

// _ ensures apiwire is referenced even if a refactor drops a call.
var _ = apiwire.RequestID

// reqIDFromCtx is a thin wrapper so handlers can log the request ID
// without importing apiwire in every file.
func reqIDFromCtx(r *http.Request) string {
	return apiwire.RequestIDFromContext(r.Context())
}
