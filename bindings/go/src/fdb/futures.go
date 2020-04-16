/*
 * futures.go
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

//  #cgo LDFLAGS: -lfdb_c -lm
//  #define FDB_API_VERSION 630
//  #include <foundationdb/fdb_c.h>
//  #include <string.h>
//
//  extern void unlockMutex(void*);
//
//  void go_callback(FDBFuture* f, void* m) {
//      unlockMutex(m);
//  }
//
//  void go_set_callback(void* f, void* m) {
//      fdb_future_set_callback(f, (FDBCallback)&go_callback, m);
//  }
import "C"

import (
	"sync"
	"unsafe"
)

// A Future represents a value (or error) to be available at some later
// time. Asynchronous FDB API functions return one of the types that implement
// the Future interface. All Future types additionally implement Get and MustGet
// methods with different return types. Calling BlockUntilReady, Get or MustGet
// will block the calling goroutine until the Future is ready.
type Future interface {
	// BlockUntilReady blocks the calling goroutine until the future is ready. A
	// future becomes ready either when it receives a value of its enclosed type
	// (if any) or is set to an error state.
	BlockUntilReady()

	// IsReady returns true if the future is ready, and false otherwise, without
	// blocking. A future is ready either when has received a value of its
	// enclosed type (if any) or has been set to an error state.
	IsReady() bool

	// Cancel cancels a future and its associated asynchronous operation. If
	// called before the future becomes ready, attempts to access the future
	// will return an error. Cancel has no effect if the future is already
	// ready.
	//
	// Note that even if a future is not ready, the associated asynchronous
	// operation may already have completed and be unable to be cancelled.
	Cancel()
}

type future struct {
	ptr *C.FDBFuture
}

func newFuture(ptr *C.FDBFuture) *future {
	return &future{ptr}
}

// Note: This function guarantees the callback will be executed **at most once**.
func fdb_future_block_until_ready(f *C.FDBFuture) {
	if C.fdb_future_is_ready(f) != 0 {
		return
	}

	// The mutex here is used as a signal that the callback is complete.
	// We first lock it, then pass it to the callback, and then lock it
	// again. The second call to lock won't return until the callback has
	// fired.
	//
	// See https://groups.google.com/forum/#!topic/golang-nuts/SPjQEcsdORA
	// for the history of why this pattern came to be used.
	m := &sync.Mutex{}
	m.Lock()
	C.go_set_callback(unsafe.Pointer(f), unsafe.Pointer(m))
	m.Lock()
}

func (f *future) BlockUntilReady() {
	fdb_future_block_until_ready(f.ptr)
}

func (f *future) IsReady() bool {
	return C.fdb_future_is_ready(f.ptr) != 0
}

func (f *future) Cancel() {
	C.fdb_future_cancel(f.ptr)
}

// FutureByteSlice represents the asynchronous result of a function that returns
// a value from a database. FutureByteSlice is a lightweight object that may be
// efficiently copied, and is safe for concurrent use by multiple goroutines.
type FutureByteSlice interface {
	// Get returns a database value (or nil if there is no value), or an error
	// if the asynchronous operation associated with this future did not
	// successfully complete. The current goroutine will be blocked until the
	// future is ready.
	Get() ([]byte, error)

	// MustGet returns a database value (or nil if there is no value), or panics
	// if the asynchronous operation associated with this future did not
	// successfully complete. The current goroutine will be blocked until the
	// future is ready.
	MustGet() []byte

	Future
}

type futureByteSlice struct {
	*future
	v []byte
	e error
	o sync.Once
}

func (f *futureByteSlice) Get() ([]byte, error) {
	f.o.Do(func() {
		defer C.fdb_future_destroy(f.ptr)

		var present C.fdb_bool_t
		var value *C.uint8_t
		var length C.int

		f.BlockUntilReady()

		if err := C.fdb_future_get_value(f.ptr, &present, &value, &length); err != 0 {
			f.e = Error{int(err)}
			return
		}

		if present != 0 {
			// Copy the native `value` into a Go byte slice so the underlying
			// native Future can be freed. This avoids the need for finalizers.
			valueDestination := make([]byte, length)
			valueSource := C.GoBytes(unsafe.Pointer(value), length)
			copy(valueDestination, valueSource)

			f.v = valueDestination
		}
	})

	return f.v, f.e
}

func (f *futureByteSlice) MustGet() []byte {
	val, err := f.Get()
	if err != nil {
		panic(err)
	}
	return val
}

// FutureKey represents the asynchronous result of a function that returns a key
// from a database. FutureKey is a lightweight object that may be efficiently
// copied, and is safe for concurrent use by multiple goroutines.
type FutureKey interface {
	// Get returns a database key or an error if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	Get() (Key, error)

	// MustGet returns a database key, or panics if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	MustGet() Key

	Future
}

type futureKey struct {
	*future
	k Key
	e error
	o sync.Once
}

func (f *futureKey) Get() (Key, error) {
	f.o.Do(func() {
		defer C.fdb_future_destroy(f.ptr)

		var value *C.uint8_t
		var length C.int

		f.BlockUntilReady()

		if err := C.fdb_future_get_key(f.ptr, &value, &length); err != 0 {
			f.e = Error{int(err)}
			return
		}

		keySource := C.GoBytes(unsafe.Pointer(value), length)
		keyDestination := make([]byte, length)
		copy(keyDestination, keySource)

		f.k = keyDestination
	})

	return f.k, f.e
}

func (f *futureKey) MustGet() Key {
	val, err := f.Get()
	if err != nil {
		panic(err)
	}
	return val
}

// FutureNil represents the asynchronous result of a function that has no return
// value. FutureNil is a lightweight object that may be efficiently copied, and
// is safe for concurrent use by multiple goroutines.
type FutureNil interface {
	// Get returns an error if the asynchronous operation associated with this
	// future did not successfully complete. The current goroutine will be
	// blocked until the future is ready.
	Get() error

	// MustGet panics if the asynchronous operation associated with this future
	// did not successfully complete. The current goroutine will be blocked
	// until the future is ready.
	MustGet()

	Future
}

type futureNil struct {
	*future
	o sync.Once
	e error
}

func (f *futureNil) Get() error {
	f.o.Do(func() {
		defer C.fdb_future_destroy(f.ptr)

		f.BlockUntilReady()
		if err := C.fdb_future_get_error(f.ptr); err != 0 {
			f.e = Error{int(err)}
		}
	})

	return f.e
}

func (f *futureNil) MustGet() {
	if err := f.Get(); err != nil {
		panic(err)
	}
}

type futureKeyValueArray struct {
	*future
}

func stringRefToSlice(ptr unsafe.Pointer) []byte {
	size := *((*C.int)(unsafe.Pointer(uintptr(ptr) + 8)))

	if size == 0 {
		return []byte{}
	}

	src := unsafe.Pointer(*(**C.uint8_t)(unsafe.Pointer(ptr)))

	return C.GoBytes(src, size)
}

func (f *futureKeyValueArray) Get() ([]KeyValue, bool, error) {
	f.BlockUntilReady()

	var kvs *C.FDBKeyValue
	var count C.int
	var more C.fdb_bool_t

	if err := C.fdb_future_get_keyvalue_array(f.ptr, &kvs, &count, &more); err != 0 {
		return nil, false, Error{int(err)}
	}

	// To minimize the number of individual allocations, we first calculate the
	// final size used by all keys and values returned from this iteration,
	// then perform one larger allocation and slice within it.

	poolSize := 0
	for i := 0; i < int(count); i++ {
		kvptr := unsafe.Pointer(uintptr(unsafe.Pointer(kvs)) + uintptr(i*24))

		poolSize += len(stringRefToSlice(kvptr))
		poolSize += len(stringRefToSlice(unsafe.Pointer(uintptr(kvptr) + 12)))
	}

	poolOffset := 0
	pool := make([]byte, poolSize)

	ret := make([]KeyValue, int(count))

	for i := 0; i < int(count); i++ {
		kvptr := unsafe.Pointer(uintptr(unsafe.Pointer(kvs)) + uintptr(i*24))

		keySource := stringRefToSlice(kvptr)
		valueSource := stringRefToSlice(unsafe.Pointer(uintptr(kvptr) + 12))

		keyDestination := pool[poolOffset : poolOffset+len(keySource)]
		poolOffset += len(keySource)

		valueDestination := pool[poolOffset : poolOffset+len(valueSource)]
		poolOffset += len(valueSource)

		copy(keyDestination, keySource)
		copy(valueDestination, valueSource)

		ret[i] = KeyValue{
			Key:   keyDestination,
			Value: valueDestination,
		}
	}

	return ret, (more != 0), nil
}

// FutureInt64 represents the asynchronous result of a function that returns a
// database version. FutureInt64 is a lightweight object that may be efficiently
// copied, and is safe for concurrent use by multiple goroutines.
type FutureInt64 interface {
	// Get returns a database version or an error if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	Get() (int64, error)

	// MustGet returns a database version, or panics if the asynchronous
	// operation associated with this future did not successfully complete. The
	// current goroutine will be blocked until the future is ready.
	MustGet() int64

	Future
}

type futureInt64 struct {
	*future
	o sync.Once
	e error
	v int64
}

func (f *futureInt64) Get() (int64, error) {
	f.o.Do(func() {
		defer C.fdb_future_destroy(f.ptr)

		f.BlockUntilReady()

		var ver C.int64_t
		if err := C.fdb_future_get_int64(f.ptr, &ver); err != 0 {
			f.v = 0
			f.e = Error{int(err)}
			return
		}

		f.v = int64(ver)
	})

	return f.v, f.e
}

func (f *futureInt64) MustGet() int64 {
	val, err := f.Get()
	if err != nil {
		panic(err)
	}
	return val
}

// FutureStringSlice represents the asynchronous result of a function that
// returns a slice of strings. FutureStringSlice is a lightweight object that
// may be efficiently copied, and is safe for concurrent use by multiple
// goroutines.
type FutureStringSlice interface {
	// Get returns a slice of strings or an error if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	Get() ([]string, error)

	// MustGet returns a slice of strings or panics if the asynchronous
	// operation associated with this future did not successfully complete. The
	// current goroutine will be blocked until the future is ready.
	MustGet() []string

	Future
}

type futureStringSlice struct {
	*future
	o sync.Once
	e error
	v []string
}

func (f *futureStringSlice) Get() ([]string, error) {
	f.o.Do(func() {
		defer C.fdb_future_destroy(f.ptr)

		f.BlockUntilReady()

		var strings **C.char
		var count C.int

		if err := C.fdb_future_get_string_array(f.ptr, (***C.char)(unsafe.Pointer(&strings)), &count); err != 0 {
			f.e = Error{int(err)}
			return
		}

		ret := make([]string, int(count))

		for i := 0; i < int(count); i++ {
			source := C.GoString((*C.char)(*(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(strings)) + uintptr(i*8)))))

			destination := make([]byte, len(source))
			copy(destination, source)

			ret[i] = string(destination)
		}

		f.v = ret
	})

	return f.v, f.e
}

func (f *futureStringSlice) MustGet() []string {
	val, err := f.Get()
	if err != nil {
		panic(err)
	}
	return val
}
