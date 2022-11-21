/*
 * directory.go
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

package main

import (
	"bytes"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func (sm *StackMachine) popTuples(count int) []tuple.Tuple {
	tuples := make([]tuple.Tuple, count)

	for i := 0; i < count; i++ {
		c := sm.waitAndPop().item.(int64)
		tuples[i] = make(tuple.Tuple, c)
		for j := int64(0); j < c; j++ {
			tuples[i][j] = sm.waitAndPop().item
		}
	}

	return tuples
}

func tupleToPath(t tuple.Tuple) []string {
	ret := make([]string, len(t))
	for i, el := range t {
		ret[i] = el.(string)
	}
	return ret
}

func tuplePackStrings(s []string) []byte {
	t := make(tuple.Tuple, len(s))
	for i, el := range s {
		t[i] = el
	}
	return t.Pack()
}

type DirectoryExtension struct {
	list       []interface{}
	index      int64
	errorIndex int64
}

func newDirectoryExtension() *DirectoryExtension {
	de := DirectoryExtension{}
	de.store(directory.Root())
	return &de
}

func (de *DirectoryExtension) store(el interface{}) {
	de.list = append(de.list, el)
}

func (de *DirectoryExtension) cwd() directory.Directory {
	return de.list[de.index].(directory.Directory)
}

func (de *DirectoryExtension) css() subspace.Subspace {
	return de.list[de.index].(subspace.Subspace)
}

func (sm *StackMachine) maybePath() []string {
	count := sm.waitAndPop().item.(int64)
	var path []string
	if count > 0 {
		tuples := sm.popTuples(1)
		path = tupleToPath(tuples[0])
	}
	return path
}

var createOps = map[string]bool{
	"CREATE_SUBSPACE": true,
	"CREATE_LAYER":    true,
	"CREATE_OR_OPEN":  true,
	"CREATE":          true,
	"OPEN":            true,
	"MOVE":            true,
	"MOVE_TO":         true,
	"OPEN_SUBSPACE":   true,
}

func (de *DirectoryExtension) processOp(sm *StackMachine, op string, isDB bool, idx int, t fdb.Transactor, rt fdb.ReadTransactor) {
	defer func() {
		if r := recover(); r != nil {
			sm.store(idx, []byte("DIRECTORY_ERROR"))
			if createOps[op] {
				de.store(nil)
			}
		}
	}()

	var err error

	switch {
	case op == "CREATE_SUBSPACE":
		tuples := sm.popTuples(1)
		rp := sm.waitAndPop().item.([]byte)
		s := subspace.FromBytes(rp).Sub(tuples[0]...)
		de.store(s)
	case op == "CREATE_LAYER":
		idx1 := sm.waitAndPop().item.(int64)
		idx2 := sm.waitAndPop().item.(int64)
		amp := sm.waitAndPop().item.(int64)
		nodeSS := de.list[idx1]
		contentSS := de.list[idx2]

		if nodeSS == nil || contentSS == nil {
			de.store(nil)
		} else {
			de.store(directory.NewDirectoryLayer(nodeSS.(subspace.Subspace), contentSS.(subspace.Subspace), (amp == int64(1))))
		}
	case op == "CREATE_OR_OPEN":
		tuples := sm.popTuples(1)
		l := sm.waitAndPop().item
		var layer []byte
		if l != nil {
			layer = l.([]byte)
		}
		d, err := de.cwd().CreateOrOpen(t, tupleToPath(tuples[0]), layer)
		if err != nil {
			panic(err)
		}
		de.store(d)
	case op == "CREATE":
		tuples := sm.popTuples(1)
		l := sm.waitAndPop().item
		var layer []byte
		if l != nil {
			layer = l.([]byte)
		}
		p := sm.waitAndPop().item
		var d directory.Directory
		if p == nil {
			d, err = de.cwd().Create(t, tupleToPath(tuples[0]), layer)
		} else {
			// p.([]byte) itself may be nil, but CreatePrefix handles that appropriately
			d, err = de.cwd().CreatePrefix(t, tupleToPath(tuples[0]), layer, p.([]byte))
		}
		if err != nil {
			panic(err)
		}
		de.store(d)
	case op == "OPEN":
		tuples := sm.popTuples(1)
		l := sm.waitAndPop().item
		var layer []byte
		if l != nil {
			layer = l.([]byte)
		}
		d, err := de.cwd().Open(rt, tupleToPath(tuples[0]), layer)
		if err != nil {
			panic(err)
		}
		de.store(d)
	case op == "CHANGE":
		i := sm.waitAndPop().item.(int64)
		if de.list[i] == nil {
			i = de.errorIndex
		}
		de.index = i
	case op == "SET_ERROR_INDEX":
		de.errorIndex = sm.waitAndPop().item.(int64)
	case op == "MOVE":
		tuples := sm.popTuples(2)
		d, err := de.cwd().Move(t, tupleToPath(tuples[0]), tupleToPath(tuples[1]))
		if err != nil {
			panic(err)
		}
		de.store(d)
	case op == "MOVE_TO":
		tuples := sm.popTuples(1)
		d, err := de.cwd().MoveTo(t, tupleToPath(tuples[0]))
		if err != nil {
			panic(err)
		}
		de.store(d)
	case strings.HasPrefix(op, "REMOVE"):
		path := sm.maybePath()
		// This ***HAS*** to call Transact to ensure that any directory version
		// key set in the process of trying to remove this potentially
		// non-existent directory, in the REMOVE but not REMOVE_IF_EXISTS case,
		// doesn't end up committing the version key. (Other languages have
		// separate remove() and remove_if_exists() so don't have this tricky
		// issue).
		_, err := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
			ok, err := de.cwd().Remove(tr, path)
			if err != nil {
				panic(err)
			}
			switch op[6:] {
			case "":
				if !ok {
					panic("directory does not exist")
				}
			case "_IF_EXISTS":
			}
			return nil, nil
		})
		if err != nil {
			panic(err)
		}
	case op == "LIST":
		subs, err := de.cwd().List(rt, sm.maybePath())
		if err != nil {
			panic(err)
		}
		t := make(tuple.Tuple, len(subs))
		for i, s := range subs {
			t[i] = s
		}
		sm.store(idx, t.Pack())
	case op == "EXISTS":
		b, err := de.cwd().Exists(rt, sm.maybePath())
		if err != nil {
			panic(err)
		}
		if b {
			sm.store(idx, int64(1))
		} else {
			sm.store(idx, int64(0))
		}
	case op == "PACK_KEY":
		tuples := sm.popTuples(1)
		sm.store(idx, de.css().Pack(tuples[0]))
	case op == "UNPACK_KEY":
		t, err := de.css().Unpack(fdb.Key(sm.waitAndPop().item.([]byte)))
		if err != nil {
			panic(err)
		}
		for _, el := range t {
			sm.store(idx, el)
		}
	case op == "RANGE":
		ss := de.css().Sub(sm.popTuples(1)[0]...)
		bk, ek := ss.FDBRangeKeys()
		sm.store(idx, bk)
		sm.store(idx, ek)
	case op == "CONTAINS":
		k := sm.waitAndPop().item.([]byte)
		b := de.css().Contains(fdb.Key(k))
		if b {
			sm.store(idx, int64(1))
		} else {
			sm.store(idx, int64(0))
		}
	case op == "OPEN_SUBSPACE":
		de.store(de.css().Sub(sm.popTuples(1)[0]...))
	case op == "LOG_SUBSPACE":
		k := sm.waitAndPop().item.([]byte)
		k = append(k, tuple.Tuple{de.index}.Pack()...)
		v := de.css().Bytes()
		t.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.Set(fdb.Key(k), v)
			return nil, nil
		})
	case op == "LOG_DIRECTORY":
		rp := sm.waitAndPop().item.([]byte)
		ss := subspace.FromBytes(rp).Sub(de.index)
		k1 := ss.Pack(tuple.Tuple{"path"})
		v1 := tuplePackStrings(de.cwd().GetPath())
		k2 := ss.Pack(tuple.Tuple{"layer"})
		v2 := tuple.Tuple{de.cwd().GetLayer()}.Pack()
		k3 := ss.Pack(tuple.Tuple{"exists"})
		var v3 []byte
		exists, err := de.cwd().Exists(rt, nil)
		if err != nil {
			panic(err)
		}
		if exists {
			v3 = tuple.Tuple{1}.Pack()
		} else {
			v3 = tuple.Tuple{0}.Pack()
		}
		k4 := ss.Pack(tuple.Tuple{"children"})
		var subs []string
		if exists {
			subs, err = de.cwd().List(rt, nil)
			if err != nil {
				panic(err)
			}
		}
		v4 := tuplePackStrings(subs)
		t.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.Set(k1, v1)
			tr.Set(k2, v2)
			tr.Set(k3, v3)
			tr.Set(k4, v4)
			return nil, nil
		})
	case op == "STRIP_PREFIX":
		ba := sm.waitAndPop().item.([]byte)
		ssb := de.css().Bytes()
		if !bytes.HasPrefix(ba, ssb) {
			panic("prefix mismatch")
		}
		ba = ba[len(ssb):]
		sm.store(idx, ba)
	}
}
