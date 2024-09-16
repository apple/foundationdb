/*
 * stacktester.go
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

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const verbose bool = false

var trMap = map[string]fdb.Transaction{}
var trMapLock = sync.RWMutex{}

// Make tuples sortable by byte-order
type byBytes []tuple.Tuple

func (b byBytes) Len() int {
	return len(b)
}

func (b byBytes) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byBytes) Less(i, j int) bool {
	return bytes.Compare(b[i].Pack(), b[j].Pack()) < 0
}

func int64ToBool(i int64) bool {
	switch i {
	case 0:
		return false
	default:
		return true
	}
}

type stackEntry struct {
	item interface{}
	idx  int
}

type StackMachine struct {
	prefix      []byte
	trName      string
	stack       []stackEntry
	lastVersion int64
	threads     sync.WaitGroup
	verbose     bool
	de          *DirectoryExtension
}

func newStackMachine(prefix []byte, verbose bool) *StackMachine {
	sm := StackMachine{verbose: verbose, prefix: prefix, de: newDirectoryExtension(), trName: string(prefix[:])}
	return &sm
}

func (sm *StackMachine) waitAndPop() (ret stackEntry) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case fdb.Error:
				ret.item = []byte(tuple.Tuple{[]byte("ERROR"), []byte(fmt.Sprintf("%d", r.Code))}.Pack())
			default:
				panic(r)
			}
		}
	}()

	ret, sm.stack = sm.stack[len(sm.stack)-1], sm.stack[:len(sm.stack)-1]
	switch el := ret.item.(type) {
	case []byte:
		ret.item = el
	case int64, uint64, *big.Int, string, bool, tuple.UUID, float32, float64, tuple.Tuple, tuple.Versionstamp:
		ret.item = el
	case fdb.Key:
		ret.item = []byte(el)
	case fdb.FutureNil:
		el.MustGet()
		ret.item = []byte("RESULT_NOT_PRESENT")
	case fdb.FutureByteSlice:
		v := el.MustGet()
		if v != nil {
			ret.item = v
		} else {
			ret.item = []byte("RESULT_NOT_PRESENT")
		}
	case fdb.FutureKey:
		ret.item = []byte(el.MustGet())
	case nil:
	default:
		log.Fatalf("Don't know how to pop stack element %v %T\n", el, el)
	}
	return
}

func (sm *StackMachine) popSelector() fdb.KeySelector {
	sel := fdb.KeySelector{fdb.Key(sm.waitAndPop().item.([]byte)), int64ToBool(sm.waitAndPop().item.(int64)), int(sm.waitAndPop().item.(int64))}
	return sel
}

func (sm *StackMachine) popKeyRange() fdb.KeyRange {
	kr := fdb.KeyRange{fdb.Key(sm.waitAndPop().item.([]byte)), fdb.Key(sm.waitAndPop().item.([]byte))}
	return kr
}

func (sm *StackMachine) popRangeOptions() fdb.RangeOptions {
	ro := fdb.RangeOptions{Limit: int(sm.waitAndPop().item.(int64)), Reverse: int64ToBool(sm.waitAndPop().item.(int64)), Mode: fdb.StreamingMode(sm.waitAndPop().item.(int64) + 1)}
	return ro
}

func (sm *StackMachine) popPrefixRange() fdb.ExactRange {
	er, e := fdb.PrefixRange(sm.waitAndPop().item.([]byte))
	if e != nil {
		panic(e)
	}
	return er
}

func (sm *StackMachine) pushRange(idx int, sl []fdb.KeyValue, prefixFilter []byte) {
	var t tuple.Tuple = make(tuple.Tuple, 0, len(sl)*2)

	for _, kv := range sl {
		if prefixFilter == nil || bytes.HasPrefix(kv.Key, prefixFilter) {
			t = append(t, kv.Key)
			t = append(t, kv.Value)
		}
	}

	sm.store(idx, []byte(t.Pack()))
}

func (sm *StackMachine) store(idx int, item interface{}) {
	sm.stack = append(sm.stack, stackEntry{item, idx})
}

func tupleToString(t tuple.Tuple) string {
	var buffer bytes.Buffer
	buffer.WriteByte('(')
	for i, el := range t {
		if i > 0 {
			buffer.WriteString(", ")
		}
		switch el := el.(type) {
		case int64, uint64:
			buffer.WriteString(fmt.Sprintf("%d", el))
		case *big.Int:
			buffer.WriteString(fmt.Sprintf("%s", el))
		case []byte:
			buffer.WriteString(fmt.Sprintf("%+q", string(el)))
		case string:
			buffer.WriteString(fmt.Sprintf("%+q", el))
		case bool:
			buffer.WriteString(fmt.Sprintf("%t", el))
		case tuple.UUID:
			buffer.WriteString(hex.EncodeToString(el[:]))
		case float32, float64:
			buffer.WriteString(fmt.Sprintf("%f", el))
		case nil:
			buffer.WriteString("nil")
		case tuple.Tuple:
			buffer.WriteString(tupleToString(el))
		default:
			log.Fatalf("Don't know how to stringify tuple element %v %T\n", el, el)
		}
	}
	buffer.WriteByte(')')
	return buffer.String()
}

func (sm *StackMachine) dumpStack() {
	for i := len(sm.stack) - 1; i >= 0; i-- {
		fmt.Printf(" %d.", sm.stack[i].idx)
		el := sm.stack[i].item
		switch el := el.(type) {
		case int64, uint64:
			fmt.Printf(" %d", el)
		case *big.Int:
			fmt.Printf(" %s", el)
		case fdb.FutureNil:
			fmt.Printf(" FutureNil")
		case fdb.FutureByteSlice:
			fmt.Printf(" FutureByteSlice")
		case fdb.FutureKey:
			fmt.Printf(" FutureKey")
		case []byte:
			fmt.Printf(" %+q", string(el))
		case fdb.Key:
			fmt.Printf(" %+q", string(el))
		case string:
			fmt.Printf(" %+q", el)
		case bool:
			fmt.Printf(" %t", el)
		case tuple.Tuple:
			fmt.Printf(" %s", tupleToString(el))
		case tuple.UUID:
			fmt.Printf(" %s", hex.EncodeToString(el[:]))
		case float32, float64:
			fmt.Printf(" %f", el)
		case nil:
			fmt.Printf(" nil")
		default:
			log.Fatalf("Don't know how to dump stack element %v %T\n", el, el)
		}
		if i != 0 {
			fmt.Printf(",")
		}
	}
}

func (sm *StackMachine) executeMutation(t fdb.Transactor, f func(fdb.Transaction) (interface{}, error), isDB bool, idx int) {
	_, e := t.Transact(f)
	if e != nil {
		panic(e)
	}
	if isDB {
		sm.store(idx, []byte("RESULT_NOT_PRESENT"))
	}
}

func (sm *StackMachine) checkWatches(watches [4]fdb.FutureNil, expected bool) bool {
	for _, watch := range watches {
		if watch.IsReady() || expected {
			e := watch.Get()
			if e != nil {
				switch e := e.(type) {
				case fdb.Error:
					tr, tr_error := db.CreateTransaction()
					if tr_error != nil {
						panic(tr_error)
					}
					tr.OnError(e).MustGet()
				default:
					panic(e)
				}
			}
			if !expected {
				return false
			}
		}
	}

	return true
}

func (sm *StackMachine) testWatches() {
	for {
		_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.Set(fdb.Key("w0"), []byte("0"))
			tr.Set(fdb.Key("w2"), []byte("2"))
			tr.Set(fdb.Key("w3"), []byte("3"))
			return nil, nil
		})
		if e != nil {
			panic(e)
		}

		var watches [4]fdb.FutureNil

		_, e = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			watches[0] = tr.Watch(fdb.Key("w0"))
			watches[1] = tr.Watch(fdb.Key("w1"))
			watches[2] = tr.Watch(fdb.Key("w2"))
			watches[3] = tr.Watch(fdb.Key("w3"))

			tr.Set(fdb.Key("w0"), []byte("0"))
			tr.Clear(fdb.Key("w1"))
			return nil, nil
		})
		if e != nil {
			panic(e)
		}

		time.Sleep(5 * time.Second)

		if !sm.checkWatches(watches, false) {
			continue
		}

		_, e = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.Set(fdb.Key("w0"), []byte("a"))
			tr.Set(fdb.Key("w1"), []byte("b"))
			tr.Clear(fdb.Key("w2"))
			tr.BitXor(fdb.Key("w3"), []byte("\xff\xff"))
			return nil, nil
		})
		if e != nil {
			panic(e)
		}

		if sm.checkWatches(watches, true) {
			return
		}
	}
}

func (sm *StackMachine) testLocality() {
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Options().SetTimeout(60 * 1000)
		tr.Options().SetReadSystemKeys()
		boundaryKeys, e := db.LocalityGetBoundaryKeys(fdb.KeyRange{fdb.Key(""), fdb.Key("\xff\xff")}, 0, 0)
		if e != nil {
			panic(e)
		}

		for i := 0; i < len(boundaryKeys)-1; i++ {
			start := boundaryKeys[i]
			end := tr.GetKey(fdb.LastLessThan(boundaryKeys[i+1])).MustGet()

			startAddresses := tr.LocalityGetAddressesForKey(start).MustGet()
			endAddresses := tr.LocalityGetAddressesForKey(end).MustGet()

			for _, address1 := range startAddresses {
				found := false
				for _, address2 := range endAddresses {
					if address1 == address2 {
						found = true
						break
					}
				}
				if !found {
					panic("Locality not internally consistent.")
				}
			}
		}

		return nil, nil
	})

	if e != nil {
		panic(e)
	}
}

func (sm *StackMachine) logStack(entries map[int]stackEntry, prefix []byte) {
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for index, el := range entries {
			var keyt tuple.Tuple
			keyt = append(keyt, int64(index))
			keyt = append(keyt, int64(el.idx))
			pk := append(prefix, keyt.Pack()...)

			var valt tuple.Tuple
			valt = append(valt, el.item)
			pv := valt.Pack()

			vl := 40000
			if len(pv) < vl {
				vl = len(pv)
			}

			tr.Set(fdb.Key(pk), pv[:vl])
		}

		return nil, nil
	})

	if e != nil {
		panic(e)
	}
	return
}

func (sm *StackMachine) currentTransaction() fdb.Transaction {
	trMapLock.RLock()
	tr := trMap[sm.trName]
	trMapLock.RUnlock()

	return tr
}

func (sm *StackMachine) newTransactionWithLockHeld() {
	tr, e := db.CreateTransaction()

	if e != nil {
		panic(e)
	}

	trMap[sm.trName] = tr
}

func (sm *StackMachine) newTransaction() {
	trMapLock.Lock()
	sm.newTransactionWithLockHeld()
	trMapLock.Unlock()
}

func (sm *StackMachine) switchTransaction(name []byte) {
	sm.trName = string(name[:])
	trMapLock.RLock()
	_, present := trMap[sm.trName]
	trMapLock.RUnlock()
	if !present {
		trMapLock.Lock()

		_, present = trMap[sm.trName]
		if !present {
			sm.newTransactionWithLockHeld()
		}

		trMapLock.Unlock()
	}
}

func (sm *StackMachine) processInst(idx int, inst tuple.Tuple) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case fdb.Error:
				sm.store(idx, []byte(tuple.Tuple{[]byte("ERROR"), []byte(fmt.Sprintf("%d", r.Code))}.Pack()))
			default:
				panic(r)
			}
		}
	}()

	var e error

	op := inst[0].(string)
	if sm.verbose {
		fmt.Printf("%d. Instruction is %s (%v)\n", idx, op, sm.prefix)
		fmt.Printf("Stack from [")
		sm.dumpStack()
		fmt.Printf(" ] (%d)\n", len(sm.stack))
	}

	var t fdb.Transactor
	var rt fdb.ReadTransactor

	var isDB bool

	switch {
	case strings.HasSuffix(op, "_SNAPSHOT"):
		rt = sm.currentTransaction().Snapshot()
		op = op[:len(op)-9]
	case strings.HasSuffix(op, "_DATABASE"):
		t = db
		rt = db
		op = op[:len(op)-9]
		isDB = true
	default:
		t = sm.currentTransaction()
		rt = sm.currentTransaction()
	}

	switch {
	case op == "PUSH":
		sm.store(idx, inst[1])
	case op == "DUP":
		entry := sm.stack[len(sm.stack)-1]
		sm.store(entry.idx, entry.item)
	case op == "EMPTY_STACK":
		sm.stack = []stackEntry{}
		sm.stack = make([]stackEntry, 0)
	case op == "SWAP":
		idx := sm.waitAndPop().item.(int64)
		sm.stack[len(sm.stack)-1], sm.stack[len(sm.stack)-1-int(idx)] = sm.stack[len(sm.stack)-1-int(idx)], sm.stack[len(sm.stack)-1]
	case op == "POP":
		sm.stack = sm.stack[:len(sm.stack)-1]
	case op == "SUB":
		var x, y *big.Int
		switch x1 := sm.waitAndPop().item.(type) {
		case *big.Int:
			x = x1
		case int64:
			x = big.NewInt(x1)
		case uint64:
			x = new(big.Int)
			x.SetUint64(x1)
		}
		switch y1 := sm.waitAndPop().item.(type) {
		case *big.Int:
			y = y1
		case int64:
			y = big.NewInt(y1)
		case uint64:
			y = new(big.Int)
			y.SetUint64(y1)
		}

		sm.store(idx, x.Sub(x, y))
	case op == "CONCAT":
		str1 := sm.waitAndPop().item
		str2 := sm.waitAndPop().item
		switch str1.(type) {
		case string:
			sm.store(idx, str1.(string)+str2.(string))
		case []byte:
			sm.store(idx, append(str1.([]byte), str2.([]byte)...))
		default:
			panic("Invalid CONCAT parameter")
		}
	case op == "NEW_TRANSACTION":
		sm.newTransaction()
	case op == "USE_TRANSACTION":
		sm.switchTransaction(sm.waitAndPop().item.([]byte))
	case op == "ON_ERROR":
		sm.store(idx, sm.currentTransaction().OnError(fdb.Error{int(sm.waitAndPop().item.(int64))}))
	case op == "GET_READ_VERSION":
		_, e = rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			sm.lastVersion = rtr.GetReadVersion().MustGet()
			sm.store(idx, []byte("GOT_READ_VERSION"))
			return nil, nil
		})
		if e != nil {
			panic(e)
		}
	case op == "SET":
		key := fdb.Key(sm.waitAndPop().item.([]byte))
		value := sm.waitAndPop().item.([]byte)
		sm.executeMutation(t, func(tr fdb.Transaction) (interface{}, error) {
			tr.Set(key, value)
			return nil, nil
		}, isDB, idx)
	case op == "LOG_STACK":
		prefix := sm.waitAndPop().item.([]byte)

		entries := make(map[int]stackEntry)
		for len(sm.stack) > 0 {
			entries[len(sm.stack)-1] = sm.waitAndPop()
			if len(entries) == 100 {
				sm.logStack(entries, prefix)
				entries = make(map[int]stackEntry)
			}
		}

		sm.logStack(entries, prefix)
	case op == "GET":
		key := fdb.Key(sm.waitAndPop().item.([]byte))
		res, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.Get(key), nil
		})
		if e != nil {
			panic(e)
		}

		sm.store(idx, res.(fdb.FutureByteSlice))
	case op == "GET_ESTIMATED_RANGE_SIZE":
		r := sm.popKeyRange()
		_, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			_ = rtr.GetEstimatedRangeSizeBytes(r).MustGet()
			sm.store(idx, []byte("GOT_ESTIMATED_RANGE_SIZE"))
			return nil, nil
		})
		if e != nil {
			panic(e)
		}
	case op == "GET_RANGE_SPLIT_POINTS":
		r := sm.popKeyRange()
		chunkSize := sm.waitAndPop().item.(int64)
		_, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			_ = rtr.GetRangeSplitPoints(r, chunkSize).MustGet()
			sm.store(idx, []byte("GOT_RANGE_SPLIT_POINTS"))
			return nil, nil
		})
		if e != nil {
			panic(e)
		}
	case op == "COMMIT":
		sm.store(idx, sm.currentTransaction().Commit())
	case op == "RESET":
		sm.currentTransaction().Reset()
	case op == "CLEAR":
		key := fdb.Key(sm.waitAndPop().item.([]byte))
		sm.executeMutation(t, func(tr fdb.Transaction) (interface{}, error) {
			tr.Clear(key)
			return nil, nil
		}, isDB, idx)
	case op == "SET_READ_VERSION":
		sm.currentTransaction().SetReadVersion(sm.lastVersion)
	case op == "WAIT_FUTURE":
		entry := sm.waitAndPop()
		sm.store(entry.idx, entry.item)
	case op == "GET_COMMITTED_VERSION":
		sm.lastVersion, e = sm.currentTransaction().GetCommittedVersion()
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("GOT_COMMITTED_VERSION"))
	case op == "GET_APPROXIMATE_SIZE":
		_ = sm.currentTransaction().GetApproximateSize().MustGet()
		sm.store(idx, []byte("GOT_APPROXIMATE_SIZE"))
	case op == "GET_VERSIONSTAMP":
		sm.store(idx, sm.currentTransaction().GetVersionstamp())
	case op == "GET_KEY":
		sel := sm.popSelector()
		prefix := sm.waitAndPop().item.([]byte)
		res, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.GetKey(sel).MustGet(), nil
		})
		if e != nil {
			panic(e)
		}

		key := res.(fdb.Key)

		if bytes.HasPrefix(key, prefix) {
			sm.store(idx, key)
		} else if bytes.Compare(key, prefix) < 0 {
			sm.store(idx, prefix)
		} else {
			s, e := fdb.Strinc(prefix)
			if e != nil {
				panic(e)
			}
			sm.store(idx, s)
		}
	case strings.HasPrefix(op, "GET_RANGE"):
		var r fdb.Range

		switch op[9:] {
		case "_STARTS_WITH":
			r = sm.popPrefixRange()
		case "_SELECTOR":
			r = fdb.SelectorRange{sm.popSelector(), sm.popSelector()}
		case "":
			r = sm.popKeyRange()
		}

		ro := sm.popRangeOptions()
		var prefix []byte = nil
		if op[9:] == "_SELECTOR" {
			prefix = sm.waitAndPop().item.([]byte)
		}

		res, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.GetRange(r, ro).GetSliceOrPanic(), nil
		})
		if e != nil {
			panic(e)
		}

		sm.pushRange(idx, res.([]fdb.KeyValue), prefix)
	case strings.HasPrefix(op, "CLEAR_RANGE"):
		var er fdb.ExactRange

		switch op[11:] {
		case "_STARTS_WITH":
			er = sm.popPrefixRange()
		case "":
			er = sm.popKeyRange()
		}

		sm.executeMutation(t, func(tr fdb.Transaction) (interface{}, error) {
			tr.ClearRange(er)
			return nil, nil
		}, isDB, idx)
	case op == "TUPLE_PACK":
		var t tuple.Tuple
		count := sm.waitAndPop().item.(int64)
		for i := 0; i < int(count); i++ {
			t = append(t, sm.waitAndPop().item)
		}
		sm.store(idx, []byte(t.Pack()))
	case op == "TUPLE_PACK_WITH_VERSIONSTAMP":
		var t tuple.Tuple

		prefix := sm.waitAndPop().item.([]byte)
		c := sm.waitAndPop().item.(int64)
		for i := 0; i < int(c); i++ {
			t = append(t, sm.waitAndPop().item)
		}

		packed, err := t.PackWithVersionstamp(prefix)
		if err != nil && strings.Contains(err.Error(), "No incomplete") {
			sm.store(idx, []byte("ERROR: NONE"))
		} else if err != nil {
			sm.store(idx, []byte("ERROR: MULTIPLE"))
		} else {
			sm.store(idx, []byte("OK"))
			sm.store(idx, packed)
		}
	case op == "TUPLE_UNPACK":
		t, e := tuple.Unpack(fdb.Key(sm.waitAndPop().item.([]byte)))
		if e != nil {
			panic(e)
		}
		for _, el := range t {
			sm.store(idx, []byte(tuple.Tuple{el}.Pack()))
		}
	case op == "TUPLE_SORT":
		count := sm.waitAndPop().item.(int64)
		tuples := make([]tuple.Tuple, count)
		for i := 0; i < int(count); i++ {
			tuples[i], e = tuple.Unpack(fdb.Key(sm.waitAndPop().item.([]byte)))
			if e != nil {
				panic(e)
			}
		}
		sort.Sort(byBytes(tuples))
		for _, t := range tuples {
			sm.store(idx, t.Pack())
		}
	case op == "ENCODE_FLOAT":
		val_bytes := sm.waitAndPop().item.([]byte)
		var val float32
		binary.Read(bytes.NewBuffer(val_bytes), binary.BigEndian, &val)
		sm.store(idx, val)
	case op == "ENCODE_DOUBLE":
		val_bytes := sm.waitAndPop().item.([]byte)
		var val float64
		binary.Read(bytes.NewBuffer(val_bytes), binary.BigEndian, &val)
		sm.store(idx, val)
	case op == "DECODE_FLOAT":
		val := sm.waitAndPop().item.(float32)
		var ibuf bytes.Buffer
		binary.Write(&ibuf, binary.BigEndian, val)
		sm.store(idx, ibuf.Bytes())
	case op == "DECODE_DOUBLE":
		val := sm.waitAndPop().item.(float64)
		var ibuf bytes.Buffer
		binary.Write(&ibuf, binary.BigEndian, val)
		sm.store(idx, ibuf.Bytes())
	case op == "TUPLE_RANGE":
		var t tuple.Tuple
		count := sm.waitAndPop().item.(int64)
		for i := 0; i < int(count); i++ {
			t = append(t, sm.waitAndPop().item)
		}
		bk, ek := t.FDBRangeKeys()
		sm.store(idx, []byte(bk.FDBKey()))
		sm.store(idx, []byte(ek.FDBKey()))
	case op == "START_THREAD":
		newsm := newStackMachine(sm.waitAndPop().item.([]byte), verbose)
		sm.threads.Add(1)
		go func() {
			newsm.Run()
			sm.threads.Done()
		}()
	case op == "WAIT_EMPTY":
		prefix := sm.waitAndPop().item.([]byte)
		er, e := fdb.PrefixRange(prefix)
		if e != nil {
			panic(e)
		}
		db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			v := tr.GetRange(er, fdb.RangeOptions{}).GetSliceOrPanic()
			if len(v) != 0 {
				panic(fdb.Error{1020})
			}
			return nil, nil
		})
		sm.store(idx, []byte("WAITED_FOR_EMPTY"))
	case op == "READ_CONFLICT_RANGE":
		e = sm.currentTransaction().AddReadConflictRange(fdb.KeyRange{fdb.Key(sm.waitAndPop().item.([]byte)), fdb.Key(sm.waitAndPop().item.([]byte))})
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("SET_CONFLICT_RANGE"))
	case op == "WRITE_CONFLICT_RANGE":
		e = sm.currentTransaction().AddWriteConflictRange(fdb.KeyRange{fdb.Key(sm.waitAndPop().item.([]byte)), fdb.Key(sm.waitAndPop().item.([]byte))})
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("SET_CONFLICT_RANGE"))
	case op == "READ_CONFLICT_KEY":
		e = sm.currentTransaction().AddReadConflictKey(fdb.Key(sm.waitAndPop().item.([]byte)))
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("SET_CONFLICT_KEY"))
	case op == "WRITE_CONFLICT_KEY":
		e = sm.currentTransaction().AddWriteConflictKey(fdb.Key(sm.waitAndPop().item.([]byte)))
		if e != nil {
			panic(e)
		}
		sm.store(idx, []byte("SET_CONFLICT_KEY"))
	case op == "ATOMIC_OP":
		opname := strings.Replace(strings.Title(strings.Replace(strings.ToLower(sm.waitAndPop().item.(string)), "_", " ", -1)), " ", "", -1)
		key := fdb.Key(sm.waitAndPop().item.([]byte))
		ival := sm.waitAndPop().item
		value := ival.([]byte)
		sm.executeMutation(t, func(tr fdb.Transaction) (interface{}, error) {
			reflect.ValueOf(tr).MethodByName(opname).Call([]reflect.Value{reflect.ValueOf(key), reflect.ValueOf(value)})
			return nil, nil
		}, isDB, idx)
	case op == "DISABLE_WRITE_CONFLICT":
		sm.currentTransaction().Options().SetNextWriteNoWriteConflictRange()
	case op == "CANCEL":
		sm.currentTransaction().Cancel()
	case op == "UNIT_TESTS":
		db.Options().SetLocationCacheSize(100001)
		db.Options().SetMaxWatches(10001)
		db.Options().SetDatacenterId("dc_id")
		db.Options().SetMachineId("machine_id")
		db.Options().SetSnapshotRywEnable()
		db.Options().SetSnapshotRywDisable()
		db.Options().SetTransactionLoggingMaxFieldLength(1000)
		db.Options().SetTransactionTimeout(100000)
		db.Options().SetTransactionTimeout(0)
		db.Options().SetTransactionMaxRetryDelay(100)
		db.Options().SetTransactionRetryLimit(10)
		db.Options().SetTransactionRetryLimit(-1)
		db.Options().SetTransactionCausalReadRisky()
		db.Options().SetTransactionIncludePortInAddress()

		if !fdb.IsAPIVersionSelected() {
			log.Fatal("API version should be selected")
		}
		apiVersion := fdb.MustGetAPIVersion()
		if apiVersion == 0 {
			log.Fatal("API version is 0")
		}
		e1 := fdb.APIVersion(apiVersion + 1)
		if e1 != nil {
			fdbE := e1.(fdb.Error)
			if fdbE.Code != 2201 {
				panic(e1)
			}
		} else {
			log.Fatal("Was not stopped from selecting two API versions")
		}
		e2 := fdb.APIVersion(apiVersion - 1)
		if e2 != nil {
			fdbE := e2.(fdb.Error)
			if fdbE.Code != 2201 {
				panic(e2)
			}
		} else {
			log.Fatal("Was not stopped from selecting two API versions")
		}
		fdb.MustAPIVersion(apiVersion)

		_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.Options().SetPrioritySystemImmediate()
			tr.Options().SetPriorityBatch()
			tr.Options().SetCausalReadRisky()
			tr.Options().SetCausalWriteRisky()
			tr.Options().SetReadYourWritesDisable()
			tr.Options().SetReadSystemKeys()
			tr.Options().SetAccessSystemKeys()
			tr.Options().SetTransactionLoggingMaxFieldLength(1000)
			tr.Options().SetTimeout(60 * 1000)
			tr.Options().SetRetryLimit(50)
			tr.Options().SetMaxRetryDelay(100)
			tr.Options().SetUsedDuringCommitProtectionDisable()
			tr.Options().SetDebugTransactionIdentifier("my_transaction")
			tr.Options().SetLogTransaction()
			tr.Options().SetReadLockAware()
			tr.Options().SetLockAware()
			tr.Options().SetIncludePortInAddress()

			return tr.Get(fdb.Key("\xff")).MustGet(), nil
		})

		if e != nil {
			panic(e)
		}

		sm.testWatches()
		sm.testLocality()

	case strings.HasPrefix(op, "DIRECTORY_"):
		sm.de.processOp(sm, op[10:], isDB, idx, t, rt)
	default:
		log.Fatalf("Unhandled operation %s\n", string(inst[0].([]byte)))
	}

	if sm.verbose {
		fmt.Printf("        to [")
		sm.dumpStack()
		fmt.Printf(" ] (%d)\n\n", len(sm.stack))
	}

	runtime.Gosched()
}

func (sm *StackMachine) Run() {
	r, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.GetRange(tuple.Tuple{sm.prefix}, fdb.RangeOptions{}).GetSliceOrPanic(), nil
	})
	if e != nil {
		panic(e)
	}

	instructions := r.([]fdb.KeyValue)

	for i, kv := range instructions {
		inst, _ := tuple.Unpack(fdb.Key(kv.Value))

		if sm.verbose {
			fmt.Printf("Instruction %d\n", i)
		}
		sm.processInst(i, inst)
	}

	sm.threads.Wait()
}

var db fdb.Database

func main() {
	var clusterFile string

	prefix := []byte(os.Args[1])
	if len(os.Args) > 3 {
		clusterFile = os.Args[3]
	}

	var e error
	var apiVersion int

	apiVersion, e = strconv.Atoi(os.Args[2])
	if e != nil {
		log.Fatal(e)
	}

	if fdb.IsAPIVersionSelected() {
		log.Fatal("API version already selected")
	}

	e = fdb.APIVersion(apiVersion)
	if e != nil {
		log.Fatal(e)
	}
	if fdb.MustGetAPIVersion() != apiVersion {
		log.Fatal("API version not equal to value selected")
	}

	db, e = fdb.OpenDatabase(clusterFile)
	if e != nil {
		log.Fatal(e)
	}

	sm := newStackMachine(prefix, verbose)

	sm.Run()
}
