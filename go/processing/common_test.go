package processing

import "testing"

func Test_BytesToInt32(t *testing.T) {
	in := [][]byte{
		[]byte("10.0"),
		[]byte("-10.0"),
		[]byte("0.0"),
		[]byte("11.1"),
	}
	out := []int32{
		100, -100, 0, 111,
	}

	for i := 0; i < len(in); i++ {
		if out[i] != BytesToInt32(in[i]) {
			t.Errorf("Error converting []byte to int32, expected: %d, got %d", out[i], BytesToInt32(in[i]))
		}
	}
}
