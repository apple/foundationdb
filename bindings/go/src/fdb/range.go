/*
 * range.go
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

import (
	"context"
	"fmt"
)

// KeyValue represents a single key-value pair in the database.
type KeyValue struct {
	Key   Key
	Value []byte
}

// RangeOptions specify how a database range read operation is carried
// out. RangeOptions objects are passed to GetRange methods of Database,
// Transaction and Snapshot.
//
// The zero value of RangeOptions represents the default range read
// configuration (no limit, lexicographic order, to be used as an iterator).
type RangeOptions struct {
	// Limit restricts the number of key-value pairs returned as part of a range
	// read. A value of 0 indicates no limit.
	Limit int

	// Mode sets the streaming mode of the range read, allowing the database to
	// balance latency and bandwidth for this read.
	Mode StreamingMode

	// Reverse indicates that the read should be performed in lexicographic
	// (false) or reverse lexicographic (true) order. When Reverse is true and
	// Limit is non-zero, the last Limit key-value pairs in the range are
	// returned. Reading ranges in reverse is supported natively by the
	// database and should have minimal extra cost.
	Reverse bool
}

// A Range describes all keys between a begin (inclusive) and end (exclusive)
// key selector.
type Range interface {
	// FDBRangeKeySelectors returns a pair of key selectors that describe the
	// beginning and end of a range.
	FDBRangeKeySelectors() (begin, end Selectable)
}

// An ExactRange describes all keys between a begin (inclusive) and end
// (exclusive) key. If you need to specify an ExactRange and you have only a
// Range, you must resolve the selectors returned by
// (Range).FDBRangeKeySelectors to keys using the (Transaction).GetKey method.
//
// Any object that implements ExactRange also implements Range, and may be used
// accordingly.
type ExactRange interface {
	// FDBRangeKeys returns a pair of keys that describe the beginning and end
	// of a range.
	FDBRangeKeys() (begin, end KeyConvertible)

	// An object that implements ExactRange must also implement Range
	// (logically, by returning FirstGreaterOrEqual of the keys returned by
	// FDBRangeKeys).
	Range
}

// KeyRange is an ExactRange constructed from a pair of KeyConvertibles. Note
// that the default zero-value of KeyRange specifies an empty range before all
// keys in the database.
type KeyRange struct {
	// The (inclusive) beginning of the range
	Begin KeyConvertible

	// The (exclusive) end of the range
	End KeyConvertible
}

// FDBRangeKeys allows KeyRange to satisfy the ExactRange interface.
func (kr KeyRange) FDBRangeKeys() (KeyConvertible, KeyConvertible) {
	return kr.Begin, kr.End
}

// FDBRangeKeySelectors allows KeyRange to satisfy the Range interface.
func (kr KeyRange) FDBRangeKeySelectors() (Selectable, Selectable) {
	return FirstGreaterOrEqual(kr.Begin), FirstGreaterOrEqual(kr.End)
}

// SelectorRange is a Range constructed directly from a pair of Selectable
// objects. Note that the default zero-value of SelectorRange specifies an empty
// range before all keys in the database.
type SelectorRange struct {
	Begin, End Selectable
}

// FDBRangeKeySelectors allows SelectorRange to satisfy the Range interface.
func (sr SelectorRange) FDBRangeKeySelectors() (Selectable, Selectable) {
	return sr.Begin, sr.End
}

// RangeResult is a handle to the asynchronous result of a range
// read. RangeResult is safe for concurrent use by multiple goroutines.
//
// A RangeResult should not be returned from a transactional function passed to
// the Transact method of a Transactor.
type RangeResult struct {
	t        *transaction
	sr       SelectorRange
	options  RangeOptions
	snapshot bool
}

// Get returns a slice of KeyValue objects satisfying the range
// specified in the read that returned this RangeResult, or an error if any of
// the asynchronous operations associated with this result did not successfully
// complete. The current goroutine will be blocked until all reads have
// completed.
// Using Iterator() to streamline reads is preferable for efficiency reasons.
func (rr RangeResult) Get(ctx context.Context) ([]KeyValue, error) {
	var ret []KeyValue

	ri := rr.Iterator()
	defer ri.Close()

	if rr.options.Limit != 0 {
		ri.options.Mode = StreamingModeExact
	} else {
		ri.options.Mode = StreamingModeWantAll
	}

	for {
		// prefetch even if very little happens between iterations
		kvs, err := ri.NextBatch(ctx, true)
		if err != nil {
			return nil, err
		}
		if len(kvs) == 0 {
			break
		}

		ret = append(ret, kvs...)

		if ri.options.Reverse {
			ri.sr.End = FirstGreaterOrEqual(kvs[ri.lastBatchLen-1].Key)
		} else {
			ri.sr.Begin = FirstGreaterThan(kvs[ri.lastBatchLen-1].Key)
		}
	}

	return ret, nil
}

// MustGet returns a slice of KeyValue objects satisfying the range
// specified in the read that returned this RangeResult, or panics if any of the
// asynchronous operations associated with this result did not successfully
// complete. The current goroutine will be blocked until all reads have
// completed.
func (rr RangeResult) MustGet(ctx context.Context) []KeyValue {
	kvs, err := rr.Get(ctx)
	if err != nil {
		panic(err)
	}
	return kvs
}

// Iterator returns a RangeIterator over the key-value pairs satisfying the
// range specified in the read that returned this RangeResult.
// Close() must be called on this range iterator to avoid a memory leak.
func (rr RangeResult) Iterator() *RangeIterator {
	return &RangeIterator{
		t:         rr.t,
		sr:        rr.sr,
		options:   rr.options,
		iteration: 1,
		snapshot:  rr.snapshot,
		more:      true,
	}
}

// RangeIterator returns the key-value pairs in the database (as KeyValue
// objects) satisfying the range specified in a range read. RangeIterator is
// constructed with the (RangeResult).Iterator method.
//
// You must call NextBatch and get a result without error.
//
// RangeIterator should not be copied or used concurrently from multiple
// goroutines, but multiple RangeIterators may be constructed from a single
// RangeResult and used concurrently. RangeIterator should not be returned from
// a transactional function passed to the Transact method of a Transactor.
type RangeIterator struct {
	t               *transaction
	sr              SelectorRange
	options         RangeOptions
	iteration       int
	more            bool
	lastBatchLen    int
	snapshot        bool
	prefetchedBatch *futureKeyValueArray
	prefetchOptions RangeOptions
}

// NextBatch advances the iterator to the next key-value pairs batch.
// It returns a slice of key-value pairs satisfying the range, an empty slice
// if the range has been exhausted, or an error if any of the asynchronous
// operations associated with this result did not successfully complete.
// If 'prefetch' is true, future for the next batch will be created on return and
// released when the RangeIterator is closed.
func (ri *RangeIterator) NextBatch(ctx context.Context, prefetch bool) ([]KeyValue, error) {
	if !ri.more {
		// iterator is done
		return nil, nil
	}

	var f *futureKeyValueArray
	if ri.prefetchedBatch != nil {
		// update state for the prefetched read batch
		ri.options = ri.prefetchOptions
		ri.iteration++

		f = ri.prefetchedBatch
		ri.prefetchedBatch = nil
	} else {
		f = ri.fetchNextBatch()
	}

	var err error
	var kvs []KeyValue
	kvs, ri.more, err = f.Get(ctx)
	ri.lastBatchLen = len(kvs)
	f.Close()
	if err != nil {
		// cannot re-use iterator after an error
		ri.more = false
		return nil, err
	}

	// if limit is zero then we can rely on the 'more' value returned by the C function
	if ri.more && ri.options.Limit != 0 {
		// if limit is non-zero then keep reading until Limit is zero
		ri.more = ri.lastBatchLen < ri.options.Limit
	}

	if prefetch && ri.more {
		ri.prefetchOptions = ri.options
		if ri.prefetchOptions.Limit > 0 {
			ri.prefetchOptions.Limit -= ri.lastBatchLen
		}
		f := ri.t.doGetRange(ri.sr, ri.prefetchOptions, ri.snapshot, ri.iteration+1)
		ri.prefetchedBatch = &f
	}

	return kvs, nil
}

func (ri *RangeIterator) fetchNextBatch() *futureKeyValueArray {
	if ri.options.Limit > 0 {
		// Not worried about this being zero, equality was checked by caller
		ri.options.Limit -= ri.lastBatchLen
	}

	ri.iteration++

	f := ri.t.doGetRange(ri.sr, ri.options, ri.snapshot, ri.iteration)
	return &f
}

func (ri *RangeIterator) Close() {
	if ri.prefetchedBatch != nil {
		ri.prefetchedBatch.Close()
	}
}

// Strinc returns the first key that would sort outside the range prefixed by
// prefix, or an error if prefix is empty or contains only 0xFF bytes.
func Strinc(prefix []byte) ([]byte, error) {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xFF {
			ret := make([]byte, i+1)
			copy(ret, prefix[:i+1])
			ret[i]++
			return ret, nil
		}
	}

	return nil, fmt.Errorf("Key must contain at least one byte not equal to 0xFF")
}

// PrefixRange returns the KeyRange describing the range of keys k such that
// bytes.HasPrefix(k, prefix) is true. PrefixRange returns an error if prefix is
// empty or entirely 0xFF bytes.
//
// Do not use PrefixRange on objects that already implement the Range or
// ExactRange interfaces. The prefix range of the byte representation of these
// objects may not correspond to their logical range.
func PrefixRange(prefix []byte) (KeyRange, error) {
	begin := make([]byte, len(prefix))
	copy(begin, prefix)
	end, err := Strinc(begin)
	if err != nil {
		return KeyRange{}, err
	}
	return KeyRange{Key(begin), Key(end)}, nil
}
