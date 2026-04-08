package kubemq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseHost(t *testing.T) {
	assert.Equal(t, "localhost", parseHost("localhost:50000"))
	assert.Equal(t, "192.168.1.1", parseHost("192.168.1.1:50000"))
	assert.Equal(t, "invalid", parseHost("invalid"))
}

func TestParsePort(t *testing.T) {
	assert.Equal(t, 50000, parsePort("localhost:50000"))
	assert.Equal(t, 8080, parsePort("localhost:8080"))
	assert.Equal(t, 50000, parsePort("invalid"))
}
