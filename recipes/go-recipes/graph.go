/*
 * graph.go
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
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const API_VERSION int = 800

func clear_subspace(trtr fdb.Transactor, sub subspace.Subspace) error {
	_, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(sub)
		return nil, nil
	})
	return err
}

type Graph struct {
	Dir, EdgeSpace, InvSpace subspace.Subspace
}

func (graph *Graph) NewGraph(dir subspace.Subspace, name string) {
	graph.Dir = dir
	ss := dir.Sub(name)
	graph.EdgeSpace = ss.Sub("edge")
	graph.InvSpace = ss.Sub("inv")
}

func (graph *Graph) set_edge(trtr fdb.Transactor, node, neighbor int) (inter interface{}, err error) {
	inter, err = trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(graph.EdgeSpace.Pack(tuple.Tuple{node, neighbor}), []byte(""))
		tr.Set(graph.InvSpace.Pack(tuple.Tuple{neighbor, node}), []byte(""))
		return nil, nil
	})
	return
}

func (graph *Graph) del_edge(trtr fdb.Transactor, node, neighbor int) (inter interface{}, err error) {
	inter, err = trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Clear(graph.EdgeSpace.Pack(tuple.Tuple{node, neighbor}))
		tr.Clear(graph.InvSpace.Pack(tuple.Tuple{neighbor, node}))
		return nil, nil
	})
	return
}

func (graph *Graph) get_out_neighbors(trtr fdb.Transactor, node int) ([]int, error) {

	val, err := trtr.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {

		kr, err := fdb.PrefixRange(graph.EdgeSpace.Pack(tuple.Tuple{node}))
		if err != nil {
			return nil, err
		}

		ri := rtr.GetRange(kr, fdb.RangeOptions{}).Iterator()
		neighbors := make([]int, 0)

		for ri.Advance() {

			kv := ri.MustGet()

			t, err := graph.EdgeSpace.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}

			neighbors = append(neighbors, int(t[1].(int64)))
		}

		return neighbors, nil
	})
	return val.([]int), err
}

func (graph *Graph) get_in_neighbors(trtr fdb.Transactor, node int) ([]int, error) {
	val, err := trtr.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {

		kr, err := fdb.PrefixRange(graph.InvSpace.Pack(tuple.Tuple{node}))
		if err != nil {
			return nil, err
		}

		ri := rtr.GetRange(kr, fdb.RangeOptions{}).Iterator()
		neighbors := make([]int, 0)

		for ri.Advance() {
			kv := ri.MustGet()

			t, err := graph.InvSpace.Unpack(kv.Key)

			if err != nil {
				return nil, err
			}

			neighbors = append(neighbors, int(t[1].(int64)))
		}

		return neighbors, nil
	})
	return val.([]int), err
}

func main() {
	fdb.MustAPIVersion(API_VERSION)

	db := fdb.MustOpenDefault()

	GraphDemoDir, err := directory.CreateOrOpen(db, []string{"Graph"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	clear_subspace(db, GraphDemoDir)

	var g Graph

	g.NewGraph(GraphDemoDir, "testgraph")

	g.set_edge(db, 0, 1)
	g.set_edge(db, 0, 2)
	g.set_edge(db, 1, 2)

	i, err := g.get_out_neighbors(db, 0)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(i)

	_, err = g.del_edge(db, 0, 2)
	if err != nil {
		log.Fatal(err)
	}

	i, err = g.get_in_neighbors(db, 2)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(i)

	clear_subspace(db, GraphDemoDir)

	i, err = g.get_in_neighbors(db, 2)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(i)
}
