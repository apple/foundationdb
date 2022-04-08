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
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func ExampleOpenDefault() {
	var e error

	e = fdb.APIVersion(720)
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

	_ = db

	// Output:
}

func TestVersionstamp(t *testing.T) {
	fdb.MustAPIVersion(720)
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
	fdb.MustAPIVersion(720)
	db := fdb.MustOpenDefault()
	_, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		rtr.Options().SetAccessSystemKeys();
		return rtr.Get(fdb.Key("\xff/")).MustGet(), nil
	})
	if e != nil {
		t.Errorf("Failed to read system key: %s", e)
	}
}


func ExampleTransactor() {
	fdb.MustAPIVersion(720)
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
	fdb.MustAPIVersion(720)
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
	fdb.MustAPIVersion(720)
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
	fdb.MustAPIVersion(720)
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
