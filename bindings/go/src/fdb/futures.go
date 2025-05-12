/*
 * futures.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
//  #define FDB_API_VERSION 740
//  #include <foundationdb/fdb_c.h>
//  #include <string.h>
//
//  extern void goFutureReadyCallback(void*, void*);
//
//  void c_future_ready_callback(FDBFuture* f, void* rs) {
//      goFutureReadyCallback(f, rs);
//  }
//
//  void c_set_callback(void* f, void* rs) {
//      fdb_future_set_callback(f, (FDBCallback)&c_future_ready_callback, rs);
//  }
import "C"

import (
	"context"
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
	BlockUntilReady(context.Context) error

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

	// Close will release resources associated with this future.
	// It must always be called, and called exactly once.
	Close()
}

type future struct {
	ptr *C.FDBFuture
}

// newFuture returns a future which must be explicitly destroyed with a call to destroy().
func newFuture(ptr *C.FDBFuture) *future {
	f := &future{
		ptr: ptr,
	}
	return f
}

type readySignal chan (struct{})

// BlockUntilReady is a Go re-implementation of fdb_future_block_until_ready.
// Note: This function guarantees the callback will be executed **at most once**.
func (f *future) BlockUntilReady(ctx context.Context) error {
	if C.fdb_future_is_ready(f.ptr) != 0 {
		return nil
	}

	// The channel here is used as a signal that the callback is complete.
	// The callback is responsible for closing it, and this function returns
	// only after that has happened.
	//
	// See also: https://groups.google.com/forum/#!topic/golang-nuts/SPjQEcsdORA
	rs := make(readySignal)
	C.c_set_callback(unsafe.Pointer(f.ptr), unsafe.Pointer(&rs))

	select {
	case <-rs:
		return nil
	case <-ctx.Done():
		C.fdb_future_cancel(f.ptr)

		return ctx.Err()
	}
}

func (f *future) IsReady() bool {
	return C.fdb_future_is_ready(f.ptr) != 0
}

func (f *future) Cancel() {
	C.fdb_future_cancel(f.ptr)
}

// Close must be explicitly called for each future to avoid a memory leak.
func (f *future) Close() {
	C.fdb_future_destroy(f.ptr)
}

// FutureByteSlice represents the asynchronous result of a function that returns
// a value from a database. FutureByteSlice is a lightweight object that may be
// efficiently copied, and is safe for concurrent use by multiple goroutines.
type FutureByteSlice interface {
	// Get returns a database value (or nil if there is no value), or an error
	// if the asynchronous operation associated with this future did not
	// successfully complete. The current goroutine will be blocked until the
	// future is ready.
	Get(context.Context) ([]byte, error)

	// MustGet returns a database value (or nil if there is no value), or panics
	// if the asynchronous operation associated with this future did not
	// successfully complete. The current goroutine will be blocked until the
	// future is ready.
	MustGet(context.Context) []byte

	Future
}

type futureByteSlice struct {
	*future
	v []byte
	e error
	o sync.Once
}

func (f *futureByteSlice) Get(ctx context.Context) ([]byte, error) {
	f.o.Do(func() {
		var present C.fdb_bool_t
		var value *C.uint8_t
		var length C.int

		err := f.BlockUntilReady(ctx)
		if err != nil {
			f.e = err
			return
		}

		if err := C.fdb_future_get_value(f.ptr, &present, &value, &length); err != 0 {
			f.e = Error{int(err)}
			return
		}

		if present != 0 {
			f.v = C.GoBytes(unsafe.Pointer(value), length)
		}

		C.fdb_future_release_memory(f.ptr)
	})

	return f.v, f.e
}

func (f *futureByteSlice) MustGet(ctx context.Context) []byte {
	val, err := f.Get(ctx)
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
	Get(context.Context) (Key, error)

	// MustGet returns a database key, or panics if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	MustGet(context.Context) Key

	Future
}

type futureKey struct {
	*future
	k Key
	e error
	o sync.Once
}

func (f *futureKey) Get(ctx context.Context) (Key, error) {
	f.o.Do(func() {
		var value *C.uint8_t
		var length C.int

		err := f.BlockUntilReady(ctx)
		if err != nil {
			f.e = err
			return
		}

		if err := C.fdb_future_get_key(f.ptr, &value, &length); err != 0 {
			f.e = Error{int(err)}
			return
		}

		f.k = C.GoBytes(unsafe.Pointer(value), length)
		C.fdb_future_release_memory(f.ptr)
	})

	return f.k, f.e
}

func (f *futureKey) MustGet(ctx context.Context) Key {
	val, err := f.Get(ctx)
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
	Get(context.Context) error

	// MustGet panics if the asynchronous operation associated with this future
	// did not successfully complete. The current goroutine will be blocked
	// until the future is ready.
	MustGet(context.Context)

	Future
}

type futureNil struct {
	*future
}

func (f *futureNil) Get(ctx context.Context) error {
	err := f.BlockUntilReady(ctx)
	if err != nil {
		return err
	}
	if err := C.fdb_future_get_error(f.ptr); err != 0 {
		return Error{int(err)}
	}

	return nil
}

func (f *futureNil) MustGet(ctx context.Context) {
	if err := f.Get(ctx); err != nil {
		panic(err)
	}
}

type futureKeyValueArray struct {
	*future
}

//go:nocheckptr
func stringRefToSlice(ptr unsafe.Pointer) []byte {
	size := *((*C.int)(unsafe.Pointer(uintptr(ptr) + 8)))

	if size == 0 {
		return []byte{}
	}

	src := unsafe.Pointer(*(**C.uint8_t)(unsafe.Pointer(ptr)))

	return C.GoBytes(src, size)
}

func (f *futureKeyValueArray) Get(ctx context.Context) ([]KeyValue, bool, error) {
	if err := f.BlockUntilReady(ctx); err != nil {
		return nil, false, err
	}

	var kvs *C.FDBKeyValue
	var count C.int
	var more C.fdb_bool_t

	if err := C.fdb_future_get_keyvalue_array(f.ptr, &kvs, &count, &more); err != 0 {
		return nil, false, Error{int(err)}
	}

	ret := make([]KeyValue, int(count))

	for i := 0; i < int(count); i++ {
		kvptr := unsafe.Pointer(uintptr(unsafe.Pointer(kvs)) + uintptr(i*24))

		ret[i].Key = stringRefToSlice(kvptr)
		ret[i].Value = stringRefToSlice(unsafe.Pointer(uintptr(kvptr) + 12))
	}

	return ret, (more != 0), nil
}

// FutureKeyArray represents the asynchronous result of a function
// that returns an array of keys. FutureKeyArray is a lightweight object
// that may be efficiently copied, and is safe for concurrent use by multiple goroutines.
type FutureKeyArray interface {

	// Get returns an array of keys or an error if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	Get(context.Context) ([]Key, error)

	// MustGet returns an array of keys, or panics if the asynchronous operations
	// associated with this future did not successfully complete. The current goroutine
	// will be blocked until the future is ready.
	MustGet(context.Context) []Key
}

type futureKeyArray struct {
	*future
}

func (f *futureKeyArray) Get(ctx context.Context) ([]Key, error) {
	if err := f.BlockUntilReady(ctx); err != nil {
		return nil, err
	}

	var ks *C.FDBKey
	var count C.int

	if err := C.fdb_future_get_key_array(f.ptr, &ks, &count); err != 0 {
		return nil, Error{int(err)}
	}

	ret := make([]Key, int(count))

	for i := 0; i < int(count); i++ {
		kptr := unsafe.Pointer(uintptr(unsafe.Pointer(ks)) + uintptr(i*12))

		ret[i] = stringRefToSlice(kptr)
	}

	return ret, nil
}

func (f *futureKeyArray) MustGet(ctx context.Context) []Key {
	val, err := f.Get(ctx)
	if err != nil {
		panic(err)
	}
	return val
}

// FutureInt64 represents the asynchronous result of a function that returns a
// database version. FutureInt64 is a lightweight object that may be efficiently
// copied, and is safe for concurrent use by multiple goroutines.
type FutureInt64 interface {
	// Get returns a database version or an error if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	Get(context.Context) (int64, error)

	// MustGet returns a database version, or panics if the asynchronous
	// operation associated with this future did not successfully complete. The
	// current goroutine will be blocked until the future is ready.
	MustGet(context.Context) int64

	Future
}

type futureInt64 struct {
	*future
}

func (f *futureInt64) Get(ctx context.Context) (int64, error) {
	if err := f.BlockUntilReady(ctx); err != nil {
		return 0, err
	}

	var ver C.int64_t
	if err := C.fdb_future_get_int64(f.ptr, &ver); err != 0 {
		return 0, Error{int(err)}
	}

	return int64(ver), nil
}

func (f *futureInt64) MustGet(ctx context.Context) int64 {
	val, err := f.Get(ctx)
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
	Get(context.Context) ([]string, error)

	// MustGet returns a slice of strings or panics if the asynchronous
	// operation associated with this future did not successfully complete. The
	// current goroutine will be blocked until the future is ready.
	MustGet(context.Context) []string

	Future
}

type futureStringSlice struct {
	*future
}

func (f *futureStringSlice) Get(ctx context.Context) ([]string, error) {
	if err := f.BlockUntilReady(ctx); err != nil {
		return nil, err
	}

	var strings **C.char
	var count C.int

	if err := C.fdb_future_get_string_array(f.ptr, (***C.char)(unsafe.Pointer(&strings)), &count); err != 0 {
		return nil, Error{int(err)}
	}

	ret := make([]string, int(count))

	for i := 0; i < int(count); i++ {
		ret[i] = C.GoString((*C.char)(*(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(strings)) + uintptr(i*8)))))
	}

	return ret, nil
}

func (f *futureStringSlice) MustGet(ctx context.Context) []string {
	val, err := f.Get(ctx)
	if err != nil {
		panic(err)
	}
	return val
}
