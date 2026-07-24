package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatus_String(t *testing.T) {
	assert.Equal(t, "granted", StatusGranted.String())
	assert.Equal(t, "waiting", StatusWaiting.String())
	assert.Equal(t, "converting", StatusConverting.String())
	assert.Equal(t, "unknown", Status(99).String())
}
