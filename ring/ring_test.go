package ring

import (
	"github.com/stretchr/testify/require"
	"math"
	"strconv"
	"testing"
)

func TestPow(t *testing.T) {
	tc := []struct {
		in, out int
	}{
		{2, 4},
		{3, 8},
		{4, 16},
		{32, 4294967296},
	}

	for _, tt := range tc {
		t.Run(strconv.Itoa(tt.in), func(t *testing.T) {
			require.Equal(t, tt.out, pow2(tt.in))
		})
	}
}

func BenchmarkPowShift(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pow2(i)
	}
}

func BenchmarkMathPow(b *testing.B) {
	var i float64
	for ; int(i) < b.N; i++ {
		math.Pow(2.0, i)
	}
}
