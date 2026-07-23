package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCT01_ValidateTLSConfig is the regression guard for CT-01: if
// exactly one of TLS_CERT_FILE/TLS_KEY_FILE is set, the server must
// refuse to start. Previously it silently fell back to plaintext HTTP,
// leaking admin tokens + txn data in cleartext.
func TestCT01_ValidateTLSConfig(t *testing.T) {
	cases := []struct {
		name    string
		cert    string
		key     string
		wantErr bool
	}{
		{"plaintext dev (both empty)", "", "", false},
		{"TLS (both set)", "/path/cert", "/path/key", false},
		{"only cert set → fail", "/path/cert", "", true},
		{"only key set → fail", "", "/path/key", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTLSConfig(tc.cert, tc.key)
			if tc.wantErr {
				require.Error(t, err, "must refuse to start with incomplete TLS config")
				assert.Contains(t, err.Error(), "incomplete TLS configuration")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
