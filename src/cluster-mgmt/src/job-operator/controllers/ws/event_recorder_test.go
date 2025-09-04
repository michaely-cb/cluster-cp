//go:build default

package ws

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFmtMemRound(t *testing.T) {
	for q, expected := range map[int64]string{
		0:               "0",
		1:               "0",
		1 << 10:         "1Ki",
		4 << 10:         "4Ki",
		1 << 20:         "1Mi",
		8 << 20:         "8Mi",
		1 << 30:         "1Gi",
		1<<30 + 500<<20: "1524Mi",
		1<<35 + 500<<20: "32Gi",
	} {
		assert.Equal(t, expected, fmtMemRound(q))
	}
}
