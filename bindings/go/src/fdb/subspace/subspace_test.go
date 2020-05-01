package subspace

import (
	"fmt"
	"testing"
)

func TestSubspaceString(t *testing.T) {
	printed := fmt.Sprint(Sub([]byte("hello"), "world", 42, 0x99))
	expected := "Subspace(rawPrefix=\\x01hello\\x00\\x02world\\x00\\x15*\\x15\\x99)"

	if printed != expected {
		t.Fatalf("printed subspace result differs, expected %v, got %v", expected, printed)
	}
}
