// main.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var db fdb.Database

func main() {

	apiVersion, err := strconv.Atoi(os.Getenv("FDB_API_VERSION"))
	if err != nil {
		log.Fatal("cannot parse FDB_API_VERSION from env")
	}
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(apiVersion)

	// Open the default database from the system cluster
	db = fdb.MustOpenDatabase(os.Getenv("FDB_CLUSTER_FILE"))

	http.HandleFunc("/counter", incrementCounter)

	fmt.Println("starting webserver")
	http.ListenAndServe(":8080", nil)
}

func incrementCounter(w http.ResponseWriter, r *http.Request) {
	ret, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		value := tr.Get(fdb.Key("my-counter")).MustGet()
		if len(value) == 0 {
			value = intToBytes(0)
		}
		counter := bytesToInt(value)
		counter++
		tr.Set(fdb.Key("my-counter"), intToBytes(counter))
		return intToBytes(counter), nil
	})

	if e != nil {
		log.Fatalf("Unable to perform FDB transaction (%v)", e)
	}

	fmt.Fprintf(w, "Counter is %d", bytesToInt(ret.([]byte)))
}

func bytesToInt(buf []byte) int {
	return int(binary.BigEndian.Uint32(buf))
}

func intToBytes(i int) []byte {
	v := uint32(i)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}
