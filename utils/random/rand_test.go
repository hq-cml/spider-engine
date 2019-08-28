package random

import "testing"

func TestRand(t *testing.T) {
	t.Log(GenRandInt(10000))
	t.Log(GenRandIntMinMax(9999, 10000))
}
