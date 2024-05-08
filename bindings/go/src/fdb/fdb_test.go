/*
 * fdb_test.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// FoundationDB Go API

package fdb_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const API_VERSION int = 730

func ExampleOpenDefault() {
	var e error

	e = fdb.APIVersion(API_VERSION)
	if e != nil {
		fmt.Printf("Unable to set API version: %v\n", e)
		return
	}

	// OpenDefault opens the database described by the platform-specific default
	// cluster file
	db, e := fdb.OpenDefault()
	if e != nil {
		fmt.Printf("Unable to open default database: %v\n", e)
		return
	}

	// Close the database after usage
	defer db.Close()

	// Do work here

	// Output:
}

func TestVersionstamp(t *testing.T) {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	setVs := func(t fdb.Transactor, key fdb.Key) (fdb.FutureKey, error) {
		fmt.Printf("setOne called with:  %T\n", t)
		ret, e := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.SetVersionstampedValue(key, []byte("blahblahbl\x00\x00\x00\x00"))
			return tr.GetVersionstamp(), nil
		})
		return ret.(fdb.FutureKey), e
	}

	getOne := func(rt fdb.ReadTransactor, key fdb.Key) ([]byte, error) {
		fmt.Printf("getOne called with: %T\n", rt)
		ret, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.Get(key).MustGet(), nil
		})
		if e != nil {
			return nil, e
		}
		return ret.([]byte), nil
	}

	var v []byte
	var fvs fdb.FutureKey
	var k fdb.Key
	var e error

	fvs, e = setVs(db, fdb.Key("foo"))
	if e != nil {
		t.Errorf("setOne failed %v", e)
	}
	v, e = getOne(db, fdb.Key("foo"))
	if e != nil {
		t.Errorf("getOne failed %v", e)
	}
	t.Logf("getOne returned %s", v)
	k, e = fvs.Get()
	if e != nil {
		t.Errorf("setOne wait failed %v", e)
	}
	t.Log(k)
	t.Logf("setOne returned %s", k)
}

func TestReadTransactionOptions(t *testing.T) {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()
	_, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		rtr.Options().SetAccessSystemKeys()
		return rtr.Get(fdb.Key("\xff/")).MustGet(), nil
	})
	if e != nil {
		t.Errorf("Failed to read system key: %s", e)
	}
}

func ExampleTransactor() {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	setOne := func(t fdb.Transactor, key fdb.Key, value []byte) error {
		fmt.Printf("setOne called with:  %T\n", t)
		_, e := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
			// We don't actually call tr.Set here to avoid mutating a real database.
			// tr.Set(key, value)
			return nil, nil
		})
		return e
	}

	setMany := func(t fdb.Transactor, value []byte, keys ...fdb.Key) error {
		fmt.Printf("setMany called with: %T\n", t)
		_, e := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
			for _, key := range keys {
				setOne(tr, key, value)
			}
			return nil, nil
		})
		return e
	}

	var e error

	fmt.Println("Calling setOne with a database:")
	e = setOne(db, []byte("foo"), []byte("bar"))
	if e != nil {
		fmt.Println(e)
		return
	}
	fmt.Println("\nCalling setMany with a database:")
	e = setMany(db, []byte("bar"), fdb.Key("foo1"), fdb.Key("foo2"), fdb.Key("foo3"))
	if e != nil {
		fmt.Println(e)
		return
	}

	// Output:
	// Calling setOne with a database:
	// setOne called with:  fdb.Database
	//
	// Calling setMany with a database:
	// setMany called with: fdb.Database
	// setOne called with:  fdb.Transaction
	// setOne called with:  fdb.Transaction
	// setOne called with:  fdb.Transaction
}

func ExampleReadTransactor() {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	getOne := func(rt fdb.ReadTransactor, key fdb.Key) ([]byte, error) {
		fmt.Printf("getOne called with: %T\n", rt)
		ret, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.Get(key).MustGet(), nil
		})
		if e != nil {
			return nil, e
		}
		return ret.([]byte), nil
	}

	getTwo := func(rt fdb.ReadTransactor, key1, key2 fdb.Key) ([][]byte, error) {
		fmt.Printf("getTwo called with: %T\n", rt)
		ret, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			r1, _ := getOne(rtr, key1)
			r2, _ := getOne(rtr.Snapshot(), key2)
			return [][]byte{r1, r2}, nil
		})
		if e != nil {
			return nil, e
		}
		return ret.([][]byte), nil
	}

	var e error

	fmt.Println("Calling getOne with a database:")
	_, e = getOne(db, fdb.Key("foo"))
	if e != nil {
		fmt.Println(e)
		return
	}
	fmt.Println("\nCalling getTwo with a database:")
	_, e = getTwo(db, fdb.Key("foo"), fdb.Key("bar"))
	if e != nil {
		fmt.Println(e)
		return
	}

	// Output:
	// Calling getOne with a database:
	// getOne called with: fdb.Database
	//
	// Calling getTwo with a database:
	// getTwo called with: fdb.Database
	// getOne called with: fdb.Transaction
	// getOne called with: fdb.Snapshot
}

func ExamplePrefixRange() {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	tr, e := db.CreateTransaction()
	if e != nil {
		fmt.Printf("Unable to create transaction: %v\n", e)
		return
	}

	// Clear and initialize data in this transaction. In examples we do not
	// commit transactions to avoid mutating a real database.
	tr.ClearRange(fdb.KeyRange{fdb.Key(""), fdb.Key{0xFF}})
	tr.Set(fdb.Key("alpha"), []byte("1"))
	tr.Set(fdb.Key("alphabetA"), []byte("2"))
	tr.Set(fdb.Key("alphabetB"), []byte("3"))
	tr.Set(fdb.Key("alphabetize"), []byte("4"))
	tr.Set(fdb.Key("beta"), []byte("5"))

	// Construct the range of all keys beginning with "alphabet". It is safe to
	// ignore the error return from PrefixRange unless the provided prefix might
	// consist entirely of zero or more 0xFF bytes.
	pr, _ := fdb.PrefixRange([]byte("alphabet"))

	// Read and process the range
	kvs, e := tr.GetRange(pr, fdb.RangeOptions{}).GetSliceWithError()
	if e != nil {
		fmt.Printf("Unable to read range: %v\n", e)
	}
	for _, kv := range kvs {
		fmt.Printf("%s: %s\n", string(kv.Key), string(kv.Value))
	}

	// Output:
	// alphabetA: 2
	// alphabetB: 3
	// alphabetize: 4
}

func ExampleRangeIterator() {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	tr, e := db.CreateTransaction()
	if e != nil {
		fmt.Printf("Unable to create transaction: %v\n", e)
		return
	}

	// Clear and initialize data in this transaction. In examples we do not
	// commit transactions to avoid mutating a real database.
	tr.ClearRange(fdb.KeyRange{fdb.Key(""), fdb.Key{0xFF}})
	tr.Set(fdb.Key("apple"), []byte("foo"))
	tr.Set(fdb.Key("cherry"), []byte("baz"))
	tr.Set(fdb.Key("banana"), []byte("bar"))

	rr := tr.GetRange(fdb.KeyRange{fdb.Key(""), fdb.Key{0xFF}}, fdb.RangeOptions{})
	ri := rr.Iterator()

	// Advance will return true until the iterator is exhausted
	for ri.Advance() {
		kv, e := ri.Get()
		if e != nil {
			fmt.Printf("Unable to read next value: %v\n", e)
			return
		}
		fmt.Printf("%s is %s\n", kv.Key, kv.Value)
	}

	// Output:
	// apple is foo
	// banana is bar
	// cherry is baz
}

func TestKeyToString(t *testing.T) {
	cases := []struct {
		key    fdb.Key
		expect string
	}{
		{fdb.Key([]byte{0}), "\\x00"},
		{fdb.Key("plain-text"), "plain-text"},
		{fdb.Key("\xbdascii☻☺"), "\\xbdascii\\xe2\\x98\\xbb\\xe2\\x98\\xba"},
	}

	for i, c := range cases {
		if s := c.key.String(); s != c.expect {
			t.Errorf("got '%v', want '%v' at case %v", s, c.expect, i)
		}
	}

	// Output:
}

func ExamplePrintable() {
	fmt.Println(fdb.Printable([]byte{0, 1, 2, 'a', 'b', 'c', '1', '2', '3', '!', '?', 255}))
	// Output: \x00\x01\x02abc123!?\xff
}

func TestDatabaseCloseRemovesResources(t *testing.T) {
	err := fdb.APIVersion(API_VERSION)
	if err != nil {
		t.Fatalf("Unable to set API version: %v\n", err)
	}

	// OpenDefault opens the database described by the platform-specific default
	// cluster file
	db, err := fdb.OpenDefault()
	if err != nil {
		t.Fatalf("Unable to set API version: %v\n", err)
	}

	// Close the database after usage
	db.Close()

	// Open the same database again, if the database is still in the cache we would return the same object, if not we create a new object with a new pointer
	newDB, err := fdb.OpenDefault()
	if err != nil {
		t.Fatalf("Unable to set API version: %v\n", err)
	}

	if db == newDB {
		t.Fatalf("Expected a different database object, got: %v and %v\n", db, newDB)
	}
}

func ExampleOpenWithConnectionString() {
	fdb.MustAPIVersion(API_VERSION)

	clusterFileContent, err := os.ReadFile(os.Getenv("FDB_CLUSTER_FILE"))
	if err != nil {
		fmt.Errorf("Unable to read cluster file: %v\n", err)
		return
	}

	// OpenWithConnectionString opens the database described by the connection string
	db, err := fdb.OpenWithConnectionString(string(clusterFileContent))
	if err != nil {
		fmt.Errorf("Unable to open database: %v\n", err)
		return
	}

	// Close the database after usage
	defer db.Close()

	// Do work here

	// Output:
}

// Copied from errors.go so that these types aren't public
var (
	errTenantNotFound    = fdb.Error{2131}
	errTenantExists      = fdb.Error{2132}
	errTenantNameInvalid = fdb.Error{2134}
)

func TestCreateTenant(t *testing.T) {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	testTenantName := fdb.Key("test-create-tenant")

	err := db.CreateTenant(testTenantName)
	if err != nil {
		t.Fatalf("Unable to create tenant: %v\n", err)
	}

	_, err = db.OpenTenant(testTenantName)
	if err != nil {
		t.Fatalf("Unable to open tenant: %v\n", err)
	}
}

func TestCreateExistTenant(t *testing.T) {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	testTenantName := fdb.Key("test-exist-tenant")

	err := db.CreateTenant(testTenantName)
	if err != nil {
		t.Fatalf("Unable to create tenant: %v\n", err)
	}

	// This should fail
	err = db.CreateTenant(testTenantName)
	assertErrorCodeEqual(t, err, errTenantExists)
}

/* FIXME: This test will not pass as FDB does not support the tenant yet
func TestOpenNotExistTenant(t *testing.T) {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	testTenantName := fdb.Key("test-not-exist-tenant")

	// this should fail
	_, err := db.OpenTenant(testTenantName)
	assertErrorCodeEqual(t, err, errTenantNotFound)
}
*/

func TestDeleteNotExistTenant(t *testing.T) {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	testTenantName := fdb.Key("test-not-exist-tenant")

	// this should fail
	err := db.DeleteTenant(testTenantName)
	assertErrorCodeEqual(t, err, errTenantNotFound)
}

func inSlice(sl []fdb.Key, t fdb.Key) bool {
	for _, s := range sl {
		if bytes.Equal(s, t) {
			return true
		}
	}
	return false
}

func assertErrorCodeEqual(t *testing.T, actual error, expected fdb.Error) {
	if actual == nil {
		t.Fatalf("Error is nil when it should be: %v\n", expected.Code)
	}

	castErr, ok := actual.(fdb.Error)
	if !ok {
		t.Fatalf("Error is wrong type %v, expected %v\n", actual, expected)
	}

	if castErr.Code != expected.Code {
		t.Fatalf("Error is wrong code, expected %v, actual %v\n", expected.Code, castErr.Code)
	}
}

func TestListTenant(t *testing.T) {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	testTenantName1 := fdb.Key("1-test-list-1-tenant-1")
	testTenantName2 := fdb.Key("2-test-list-2-tenant-2")

	err := db.CreateTenant(testTenantName1)
	if err != nil {
		t.Fatalf("Unable to create tenant 1: %v\n", err)
	}

	err = db.CreateTenant(testTenantName2)
	if err != nil {
		t.Fatalf("Unable to create tenant 2: %v\n", err)
	}

	ls, err := db.ListTenants()
	if err != nil {
		t.Fatalf("Unable to list tenants: %v\n", err)
	}

	if !inSlice(ls, testTenantName1) {
		t.Fatalf("tenant 1 not in slice %#v", ls)
	}

	if !inSlice(ls, testTenantName2) {
		t.Fatalf("tenant 2 not in slice, %#v", ls)
	}
}

func TestInvalidPrefixTenant(t *testing.T) {
	fdb.MustAPIVersion(API_VERSION)
	db := fdb.MustOpenDefault()

	testTenantName := fdb.Key("\xFFtest-invalid-prefix-tenant")

	// this should fail
	err := db.CreateTenant(testTenantName)
	assertErrorCodeEqual(t, err, errTenantNameInvalid)
}
