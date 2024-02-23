/*
 * fdb.go
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

package fdb

// #define FDB_API_VERSION 620
// #include <foundationdb/fdb_c.h>
// #include <stdlib.h>
import "C"

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"sync"
	"unsafe"
)

// Would put this in futures.go but for the documented issue with
// exports and functions in preamble
// (https://code.google.com/p/go-wiki/wiki/cgo#Global_functions)
//export unlockMutex
func unlockMutex(p unsafe.Pointer) {
	m := (*sync.Mutex)(p)
	m.Unlock()
}

// A Transactor can execute a function that requires a Transaction. Functions
// written to accept a Transactor are called transactional functions, and may be
// called with either a Database or a Transaction.
type Transactor interface {
	// Transact executes the caller-provided function, providing it with a
	// Transaction (itself a Transactor, allowing composition of transactional
	// functions).
	Transact(func(Transaction) (interface{}, error)) (interface{}, error)

	// All Transactors are also ReadTransactors, allowing them to be used with
	// read-only transactional functions.
	ReadTransactor
}

// A ReadTransactor can execute a function that requires a
// ReadTransaction. Functions written to accept a ReadTransactor are called
// read-only transactional functions, and may be called with a Database,
// Transaction or Snapshot.
type ReadTransactor interface {
	// ReadTransact executes the caller-provided function, providing it with a
	// ReadTransaction (itself a ReadTransactor, allowing composition of
	// read-only transactional functions).
	ReadTransact(func(ReadTransaction) (interface{}, error)) (interface{}, error)
}

func setOpt(setter func(*C.uint8_t, C.int) C.fdb_error_t, param []byte) error {
	if err := setter(byteSliceToPtr(param), C.int(len(param))); err != 0 {
		return Error{int(err)}
	}

	return nil
}

// NetworkOptions is a handle with which to set options that affect the entire
// FoundationDB client. A NetworkOptions instance should be obtained with the
// fdb.Options function.
type NetworkOptions struct {
}

// Options returns a NetworkOptions instance suitable for setting options that
// affect the entire FoundationDB client.
func Options() NetworkOptions {
	return NetworkOptions{}
}

func (opt NetworkOptions) setOpt(code int, param []byte) error {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return errAPIVersionUnset
	}

	return setOpt(func(p *C.uint8_t, pl C.int) C.fdb_error_t {
		return C.fdb_network_set_option(C.FDBNetworkOption(code), p, pl)
	}, param)
}

// APIVersion determines the runtime behavior the fdb package. If the requested
// version is not supported by both the fdb package and the FoundationDB C
// library, an error will be returned. APIVersion must be called prior to any
// other functions in the fdb package.
//
// Currently, this package supports API versions 200 through 620.
//
// Warning: When using the multi-version client API, setting an API version that
// is not supported by a particular client library will prevent that client from
// being used to connect to the cluster. In particular, you should not advance
// the API version of your application after upgrading your client until the
// cluster has also been upgraded.
func APIVersion(version int) error {
	headerVersion := 620

	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion != 0 {
		if apiVersion == version {
			return nil
		}
		return errAPIVersionAlreadySet
	}

	if version < 200 || version > 620 {
		return errAPIVersionNotSupported
	}

	if e := C.fdb_select_api_version_impl(C.int(version), C.int(headerVersion)); e != 0 {
		if e != 0 {
			if e == 2203 {
				maxSupportedVersion := C.fdb_get_max_api_version()
				if headerVersion > int(maxSupportedVersion) {
					return fmt.Errorf("This version of the FoundationDB Go binding is "+
						"not supported by the installed FoundationDB C library. "+
						"The binding requires a library that supports API version %d, "+
						"but the installed library supports a maximum version of %d.", headerVersion, maxSupportedVersion)
				}
				return fmt.Errorf("API version %d is not supported by the installed FoundationDB C library.", version)
			}
			return Error{int(e)}
		}
	}

	apiVersion = version

	return nil
}

// Determines if an API version has already been selected, i.e., if
// APIVersion or MustAPIVersion have already been called.
func IsAPIVersionSelected() bool {
	return apiVersion != 0
}

// Returns the API version that has been selected through APIVersion
// or MustAPIVersion. If the version has already been selected, then
// the first value returned is the API version and the error is
// nil. If the API version has not yet been set, then the error
// will be non-nil.
func GetAPIVersion() (int, error) {
	if IsAPIVersionSelected() {
		return apiVersion, nil
	}
	return 0, errAPIVersionUnset
}

// MustAPIVersion is like APIVersion but panics if the API version is not
// supported.
func MustAPIVersion(version int) {
	err := APIVersion(version)
	if err != nil {
		panic(err)
	}
}

// MustGetAPIVersion is like GetAPIVersion but panics if the API version
// has not yet been set.
func MustGetAPIVersion() int {
	apiVersion, err := GetAPIVersion()
	if err != nil {
		panic(err)
	}
	return apiVersion
}

var apiVersion int
var openDatabases sync.Map
var networkMutex sync.RWMutex
var setupNetworkOnce sync.Once

func startNetwork() error {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	var err error
	// Only setup the network once. There is no need for an additional mutex as the execution for all
	// callers will be blocked until the method has finished. The usage of networkMutex will help to
	// make sure we call the whole startNetwork only once.
	setupNetworkOnce.Do(func() {
		errno := C.fdb_setup_network()
		// Stop further execution
		if errno != 0 {
			err = fmt.Errorf("Could not setup nework, got error; %d", errno)
			return
		}

		go func() {
			e := C.fdb_run_network()
			if e != 0 {
				err = fmt.Errorf("Unhandled error in FoundationDB network thread: %d", errno)
				log.Println(err)
			}
		}()
	})

	if err != nil {
		log.Println(err)
		// We have to reset the once as the initialization was not successfull.
		// Should we panic here?
		setupNetworkOnce = sync.Once{}
		return err
	}

	return nil
}

// Deprecated: the network is started automatically when a database is opened.
// StartNetwork initializes the FoundationDB client networking engine. StartNetwork
// must not be called more than once.
func StartNetwork() error {
	if apiVersion == 0 {
		return errAPIVersionUnset
	}

	return startNetwork()
}

// DefaultClusterFile should be passed to fdb.Open to allow the FoundationDB C
// library to select the platform-appropriate default cluster file on the current machine.
const DefaultClusterFile string = ""

// OpenDefault returns a database handle to the FoundationDB cluster identified
// by the DefaultClusterFile on the current machine.
//
// A single client can use this function multiple times to connect to different
// clusters simultaneously, with each invocation requiring its own cluster file.
// To connect to multiple clusters running at different, incompatible versions,
// the multi-version client API must be used.
func OpenDefault() (Database, error) {
	return OpenDatabase(DefaultClusterFile)
}

// MustOpenDefault is like OpenDefault but panics if the default database cannot
// be opened.
func MustOpenDefault() Database {
	db, err := OpenDefault()
	if err != nil {
		panic(err)
	}
	return db
}

// Open returns a database handle to the FoundationDB cluster identified
// by the provided cluster file and database name.
//
// A single client can use this function multiple times to connect to different
// clusters simultaneously, with each invocation requiring its own cluster file.
// To connect to multiple clusters running at different, incompatible versions,
// the multi-version client API must be used.
func OpenDatabase(clusterFile string) (Database, error) {
	var db Database
	storedDB, exist := openDatabases.Load(clusterFile)
	if !exist {
		var err error
		db, err = createDatabase(clusterFile)
		if err != nil {
			return Database{}, err
		}

		openDatabases.Store(clusterFile, db)
		return db, nil
	}

	// This case shouldn't happen and is only a safeguard.
	var ok bool
	if db, ok = storedDB.(Database); !ok {
		// As the current entry is not usable, we delete it.
		openDatabases.Delete(clusterFile)
		return Database{}, fmt.Errorf("cannot load database from cache")
	}

	return db, nil
}

// MustOpenDatabase is like OpenDatabase but panics if the default database cannot
// be opened.
func MustOpenDatabase(clusterFile string) Database {
	db, err := OpenDatabase(clusterFile)
	if err != nil {
		panic(err)
	}
	return db
}

// Deprecated: Use OpenDatabase instead.
// The database name must be []byte("DB").
func Open(clusterFile string, dbName []byte) (Database, error) {
	if bytes.Compare(dbName, []byte("DB")) != 0 {
		return Database{}, Error{2013} // invalid_database_name
	}
	return OpenDatabase(clusterFile)
}

// Deprecated: Use MustOpenDatabase instead.
// MustOpen is like Open but panics if the database cannot be opened.
func MustOpen(clusterFile string, dbName []byte) Database {
	db, err := Open(clusterFile, dbName)
	if err != nil {
		panic(err)
	}
	return db
}

func createDatabase(clusterFile string) (Database, error) {
	log.Println("DEBUG: OpenDatabase")
	err := startNetwork()
	if err != nil {
		return Database{}, err
	}
	log.Println("DEBUG: OpenDatabase network started")

	var cf *C.char
	if len(clusterFile) != 0 {
		cf = C.CString(clusterFile)
		defer C.free(unsafe.Pointer(cf))
	}

	var outdb *C.FDBDatabase
	if err := C.fdb_create_database(cf, &outdb); err != 0 {
		return Database{}, Error{int(err)}
	}

	log.Println("outdb:", outdb)
	if outdb == nil {
		return Database{}, fmt.Errorf("could not create database, fdb_create_database returned nil pointer")
	}

	db := &database{outdb}
	runtime.SetFinalizer(db, (*database).destroy)

	return Database{db}, nil
}

// Deprecated: Use OpenDatabase instead.
// CreateCluster returns a cluster handle to the FoundationDB cluster identified
// by the provided cluster file.
func CreateCluster(clusterFile string) (Cluster, error) {
	networkMutex.Lock()
	defer networkMutex.Unlock()

	if apiVersion == 0 {
		return Cluster{}, errAPIVersionUnset
	}

	return Cluster{clusterFile}, nil
}

func byteSliceToPtr(b []byte) *C.uint8_t {
	if len(b) > 0 {
		return (*C.uint8_t)(unsafe.Pointer(&b[0]))
	}
	return nil
}

// A KeyConvertible can be converted to a FoundationDB Key. All functions in the
// FoundationDB API that address a specific key accept a KeyConvertible.
type KeyConvertible interface {
	FDBKey() Key
}

// Key represents a FoundationDB key, a lexicographically-ordered sequence of
// bytes. Key implements the KeyConvertible interface.
type Key []byte

// FDBKey allows Key to (trivially) satisfy the KeyConvertible interface.
func (k Key) FDBKey() Key {
	return k
}

// String describes the key as a human readable string.
func (k Key) String() string {
	return Printable(k)
}

// Printable returns a human readable version of a byte array. The bytes that correspond with
// ASCII printable characters [32-127) are passed through. Other bytes are
// replaced with \x followed by a two character zero-padded hex code for byte.
func Printable(d []byte) string {
	buf := new(bytes.Buffer)
	for _, b := range d {
		if b >= 32 && b < 127 && b != '\\' {
			buf.WriteByte(b)
			continue
		}
		if b == '\\' {
			buf.WriteString("\\\\")
			continue
		}
		buf.WriteString(fmt.Sprintf("\\x%02x", b))
	}
	return buf.String()
}

func panicToError(e *error) {
	if r := recover(); r != nil {
		fe, ok := r.(Error)
		if ok {
			*e = fe
		} else {
			panic(r)
		}
	}
}
