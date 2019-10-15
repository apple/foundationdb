package subspace

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"testing"
)

// TestSubspacePackWithVersionstamp confirms that packing Versionstamps
// in subspaces work by setting, then preparing to read back a key.
func TestSubspacePackWithVersionstamp(t *testing.T) {

	// I assume this can be lowered, but I have not tested it.
	fdb.MustAPIVersion(610)
	db := fdb.MustOpenDefault()

	var sub Subspace
	sub = FromBytes([]byte("testspace"))

	tup := tuple.Tuple{tuple.IncompleteVersionstamp(uint16(0))}
	key, err := sub.PackWithVersionstamp(tup)

	if err != nil {
		t.Errorf("PackWithVersionstamp failed: %s", err)
	}

	ret, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.SetVersionstampedKey(key, []byte("blahblahbl"))
		return tr.GetVersionstamp(), nil
	})

	if err != nil {
		t.Error("Transaction failed")
	}

	fvs := ret.(fdb.FutureKey)

	_, err = fvs.Get()

	if err != nil {
		t.Error("Failed to get the written Versionstamp")
	}

	// It would be nice to include a read back of the key here, but when
	// I started writing that part of the test, most of it was spent
	// on writing Versionstamp management in Go, which isn't really
	// fleshed out in the Go binding... So I'm going to leave that for
	// when that aspect of the binding is more developed.
}