/*
 * directory_layer.go
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

// FoundationDB Go Directory Layer

package directory

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type directoryLayer struct {
	nodeSS    subspace.Subspace
	contentSS subspace.Subspace

	allowManualPrefixes bool

	allocator highContentionAllocator
	rootNode  subspace.Subspace

	path []string
}

// NewDirectoryLayer returns a new root directory (as a Directory). The
// subspaces nodeSS and contentSS control where the directory metadata and
// contents are stored. The default root directory has a nodeSS of
// subspace.FromBytes([]byte{0xFE}) and a contentSS of
// subspace.AllKeys(). Specifying more restrictive values for nodeSS and
// contentSS will allow using the directory layer alongside other content in a
// database.
//
// If allowManualPrefixes is false, all calls to CreatePrefix on the returned
// Directory (or any subdirectories) will fail, and all directory prefixes will
// be automatically allocated. The default root directory does not allow manual
// prefixes.
func NewDirectoryLayer(nodeSS, contentSS subspace.Subspace, allowManualPrefixes bool) Directory {
	var dl directoryLayer

	dl.nodeSS = subspace.FromBytes(nodeSS.Bytes())
	dl.contentSS = subspace.FromBytes(contentSS.Bytes())

	dl.allowManualPrefixes = allowManualPrefixes

	dl.rootNode = dl.nodeSS.Sub(dl.nodeSS.Bytes())
	dl.allocator = newHCA(dl.rootNode.Sub([]byte("hca")))

	return dl
}

func (dl directoryLayer) createOrOpen(rtr fdb.ReadTransaction, tr *fdb.Transaction, path []string, layer []byte, prefix []byte, allowCreate, allowOpen bool) (DirectorySubspace, error) {
	if err := dl.checkVersion(rtr, nil); err != nil {
		return nil, err
	}

	if prefix != nil && !dl.allowManualPrefixes {
		if len(dl.path) == 0 {
			return nil, errors.New("cannot specify a prefix unless manual prefixes are enabled")
		}
		return nil, errors.New("cannot specify a prefix in a partition")
	}

	if len(path) == 0 {
		return nil, errors.New("the root directory cannot be opened")
	}

	existingNode := dl.find(rtr, path).prefetchMetadata(rtr)
	if existingNode.exists() {
		if existingNode.isInPartition(nil, false) {
			subpath := existingNode.getPartitionSubpath()
			enc, err := existingNode.getContents(dl, nil)
			if err != nil {
				return nil, err
			}
			return enc.(directoryPartition).createOrOpen(rtr, tr, subpath, layer, prefix, allowCreate, allowOpen)
		}

		if !allowOpen {
			return nil, ErrDirAlreadyExists
		}

		if layer != nil {
			if l, err := existingNode._layer.Get(); err != nil || bytes.Compare(l, layer) != 0 {
				return nil, errors.New("the directory was created with an incompatible layer")
			}
		}

		return existingNode.getContents(dl, nil)
	}

	if !allowCreate {
		return nil, ErrDirNotExists
	}

	if err := dl.checkVersion(rtr, tr); err != nil {
		return nil, err
	}

	if prefix == nil {
		newss, err := dl.allocator.allocate(*tr, dl.contentSS)
		if err != nil {
			return nil, fmt.Errorf("unable to allocate new directory prefix (%s)", err.Error())
		}

		if !isRangeEmpty(rtr, newss) {
			return nil, fmt.Errorf("the database has keys stored at the prefix chosen by the automatic prefix allocator: %v", prefix)
		}

		prefix = newss.Bytes()

		pf, err := dl.isPrefixFree(rtr.Snapshot(), prefix)
		if err != nil {
			return nil, err
		}
		if !pf {
			return nil, errors.New("the directory layer has manually allocated prefixes that conflict with the automatic prefix allocator")
		}
	} else {
		pf, err := dl.isPrefixFree(rtr, prefix)
		if err != nil {
			return nil, err
		}
		if !pf {
			return nil, errors.New("the given prefix is already in use")
		}
	}

	var parentNode subspace.Subspace

	if len(path) > 1 {
		pd, err := dl.createOrOpen(rtr, tr, path[:len(path)-1], nil, nil, true, true)
		if err != nil {
			return nil, err
		}
		parentNode = dl.nodeWithPrefix(pd.Bytes())
	} else {
		parentNode = dl.rootNode
	}

	if parentNode == nil {
		return nil, ErrParentDirDoesNotExist
	}

	node := dl.nodeWithPrefix(prefix)
	tr.Set(parentNode.Sub(_SUBDIRS, path[len(path)-1]), prefix)

	if layer == nil {
		layer = []byte{}
	}

	tr.Set(node.Sub([]byte("layer")), layer)

	return dl.contentsOfNode(node, path, layer)
}

func (dl directoryLayer) CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	r, err := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, &tr, path, layer, nil, true, true)
	})
	if err != nil {
		return nil, err
	}
	return r.(DirectorySubspace), nil
}

func (dl directoryLayer) Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	r, err := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, &tr, path, layer, nil, true, false)
	})
	if err != nil {
		return nil, err
	}
	return r.(DirectorySubspace), nil
}

func (dl directoryLayer) CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error) {
	if prefix == nil {
		prefix = []byte{}
	}
	r, err := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return dl.createOrOpen(tr, &tr, path, layer, prefix, true, false)
	})
	if err != nil {
		return nil, err
	}
	return r.(DirectorySubspace), nil
}

func (dl directoryLayer) Open(rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	r, err := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		return dl.createOrOpen(rtr, nil, path, layer, nil, false, true)
	})
	if err != nil {
		return nil, err
	}
	return r.(DirectorySubspace), nil
}

func (dl directoryLayer) Exists(rt fdb.ReadTransactor, path []string) (bool, error) {
	r, err := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		if err := dl.checkVersion(rtr, nil); err != nil {
			return false, err
		}

		node := dl.find(rtr, path).prefetchMetadata(rtr)
		if !node.exists() {
			return false, nil
		}

		if node.isInPartition(nil, false) {
			nc, err := node.getContents(dl, nil)
			if err != nil {
				return false, err
			}
			return nc.Exists(rtr, node.getPartitionSubpath())
		}

		return true, nil
	})
	if err != nil {
		return false, err
	}
	return r.(bool), nil
}

func (dl directoryLayer) List(rt fdb.ReadTransactor, path []string) ([]string, error) {
	r, err := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		if err := dl.checkVersion(rtr, nil); err != nil {
			return nil, err
		}

		node := dl.find(rtr, path).prefetchMetadata(rtr)
		if !node.exists() {
			return nil, ErrDirNotExists
		}

		if node.isInPartition(nil, true) {
			nc, err := node.getContents(dl, nil)
			if err != nil {
				return nil, err
			}
			return nc.List(rtr, node.getPartitionSubpath())
		}

		return dl.subdirNames(rtr, node.subspace)
	})
	if err != nil {
		return nil, err
	}
	return r.([]string), nil
}

func (dl directoryLayer) MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return nil, errors.New("the root directory cannot be moved")
}

func (dl directoryLayer) Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error) {
	r, err := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := dl.checkVersion(tr, &tr); err != nil {
			return nil, err
		}

		sliceEnd := len(oldPath)
		if sliceEnd > len(newPath) {
			sliceEnd = len(newPath)
		}
		if stringsEqual(oldPath, newPath[:sliceEnd]) {
			return nil, errors.New("the destination directory cannot be a subdirectory of the source directory")
		}

		oldNode := dl.find(tr, oldPath).prefetchMetadata(tr)
		newNode := dl.find(tr, newPath).prefetchMetadata(tr)

		if !oldNode.exists() {
			return nil, errors.New("the source directory does not exist")
		}

		if oldNode.isInPartition(nil, false) || newNode.isInPartition(nil, false) {
			if !oldNode.isInPartition(nil, false) || !newNode.isInPartition(nil, false) || !stringsEqual(oldNode.path, newNode.path) {
				return nil, errors.New("cannot move between partitions")
			}

			nnc, err := newNode.getContents(dl, nil)
			if err != nil {
				return nil, err
			}
			return nnc.Move(tr, oldNode.getPartitionSubpath(), newNode.getPartitionSubpath())
		}

		if newNode.exists() {
			return nil, errors.New("the destination directory already exists. Remove it first")
		}

		parentNode := dl.find(tr, newPath[:len(newPath)-1])
		if !parentNode.exists() {
			return nil, errors.New("the parent of the destination directory does not exist. Create it first")
		}

		p, err := dl.nodeSS.Unpack(oldNode.subspace)
		if err != nil {
			return nil, err
		}
		tr.Set(parentNode.subspace.Sub(_SUBDIRS, newPath[len(newPath)-1]), p[0].([]byte))

		dl.removeFromParent(tr, oldPath)

		l, err := oldNode._layer.Get()
		if err != nil {
			return nil, err
		}
		return dl.contentsOfNode(oldNode.subspace, newPath, l)
	})
	if err != nil {
		return nil, err
	}
	return r.(DirectorySubspace), nil
}

func (dl directoryLayer) Remove(t fdb.Transactor, path []string) (bool, error) {
	r, err := t.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := dl.checkVersion(tr, &tr); err != nil {
			return false, err
		}

		if len(path) == 0 {
			return false, errors.New("the root directory cannot be removed")
		}

		node := dl.find(tr, path).prefetchMetadata(tr)

		if !node.exists() {
			return false, nil
		}

		if node.isInPartition(nil, false) {
			nc, err := node.getContents(dl, nil)
			if err != nil {
				return false, err
			}
			return nc.(directoryPartition).Remove(tr, node.getPartitionSubpath())
		}

		if err := dl.removeRecursive(tr, node.subspace); err != nil {
			return false, err
		}
		dl.removeFromParent(tr, path)

		return true, nil
	})
	if err != nil {
		return false, err
	}
	return r.(bool), nil
}

func (dl directoryLayer) removeRecursive(tr fdb.Transaction, node subspace.Subspace) error {
	nodes := dl.subdirNodes(tr, node)
	for i := range nodes {
		if err := dl.removeRecursive(tr, nodes[i]); err != nil {
			return err
		}
	}

	p, err := dl.nodeSS.Unpack(node)
	if err != nil {
		return err
	}
	kr, err := fdb.PrefixRange(p[0].([]byte))
	if err != nil {
		return err
	}

	tr.ClearRange(kr)
	tr.ClearRange(node)

	return nil
}

func (dl directoryLayer) removeFromParent(tr fdb.Transaction, path []string) {
	parent := dl.find(tr, path[:len(path)-1])
	tr.Clear(parent.subspace.Sub(_SUBDIRS, path[len(path)-1]))
}

func (dl directoryLayer) GetLayer() []byte {
	return []byte{}
}

func (dl directoryLayer) GetPath() []string {
	return dl.path
}

func (dl directoryLayer) subdirNames(rtr fdb.ReadTransaction, node subspace.Subspace) ([]string, error) {
	sd := node.Sub(_SUBDIRS)

	rr := rtr.GetRange(sd, fdb.RangeOptions{})
	ri := rr.Iterator()

	var ret []string

	for ri.Advance() {
		kv, err := ri.Get()
		if err != nil {
			return nil, err
		}

		p, err := sd.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}

		ret = append(ret, p[0].(string))
	}

	return ret, nil
}

func (dl directoryLayer) subdirNodes(tr fdb.Transaction, node subspace.Subspace) []subspace.Subspace {
	sd := node.Sub(_SUBDIRS)

	rr := tr.GetRange(sd, fdb.RangeOptions{})
	ri := rr.Iterator()

	var ret []subspace.Subspace

	for ri.Advance() {
		kv := ri.MustGet()

		ret = append(ret, dl.nodeWithPrefix(kv.Value))
	}

	return ret
}

func (dl directoryLayer) nodeContainingKey(rtr fdb.ReadTransaction, key []byte) (subspace.Subspace, error) {
	if bytes.HasPrefix(key, dl.nodeSS.Bytes()) {
		return dl.rootNode, nil
	}

	bk, _ := dl.nodeSS.FDBRangeKeys()
	kr := fdb.KeyRange{bk, fdb.Key(append(dl.nodeSS.Pack(tuple.Tuple{key}), 0x00))}

	kvs, err := rtr.GetRange(kr, fdb.RangeOptions{Reverse: true, Limit: 1}).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(kvs) == 1 {
		pp, err := dl.nodeSS.Unpack(kvs[0].Key)
		if err != nil {
			return nil, err
		}
		prevPrefix := pp[0].([]byte)
		if bytes.HasPrefix(key, prevPrefix) {
			return dl.nodeWithPrefix(prevPrefix), nil
		}
	}

	return nil, nil
}

func (dl directoryLayer) isPrefixFree(rtr fdb.ReadTransaction, prefix []byte) (bool, error) {
	if len(prefix) == 0 {
		return false, nil
	}

	nck, err := dl.nodeContainingKey(rtr, prefix)
	if err != nil {
		return false, err
	}
	if nck != nil {
		return false, nil
	}

	kr, err := fdb.PrefixRange(prefix)
	if err != nil {
		return false, err
	}

	bk, ek := kr.FDBRangeKeys()
	if !isRangeEmpty(rtr, fdb.KeyRange{dl.nodeSS.Pack(tuple.Tuple{bk}), dl.nodeSS.Pack(tuple.Tuple{ek})}) {
		return false, nil
	}

	return true, nil
}

func (dl directoryLayer) checkVersion(rtr fdb.ReadTransaction, tr *fdb.Transaction) error {
	version, err := rtr.Get(dl.rootNode.Sub([]byte("version"))).Get()
	if err != nil {
		return err
	}

	if version == nil {
		if tr != nil {
			dl.initializeDirectory(*tr)
		}
		return nil
	}

	var versions []int32
	buf := bytes.NewBuffer(version)

	for i := 0; i < 3; i++ {
		var v int32
		err := binary.Read(buf, binary.LittleEndian, &v)
		if err != nil {
			return errors.New("cannot determine directory version present in database")
		}
		versions = append(versions, v)
	}

	if versions[0] > _MAJORVERSION {
		return fmt.Errorf("cannot load directory with version %d.%d.%d using directory layer %d.%d.%d", versions[0], versions[1], versions[2], _MAJORVERSION, _MINORVERSION, _MICROVERSION)
	}

	if versions[1] > _MINORVERSION && tr != nil /* aka write access allowed */ {
		return fmt.Errorf("directory with version %d.%d.%d is read-only when opened using directory layer %d.%d.%d", versions[0], versions[1], versions[2], _MAJORVERSION, _MINORVERSION, _MICROVERSION)
	}

	return nil
}

func (dl directoryLayer) initializeDirectory(tr fdb.Transaction) {
	buf := new(bytes.Buffer)

	// bytes.Buffer claims that Write will always return a nil error, which
	// means the error return here can only be an encoding issue. So long as we
	// don't set our own versions to something completely invalid, we should be
	// OK to ignore error returns.
	binary.Write(buf, binary.LittleEndian, _MAJORVERSION)
	binary.Write(buf, binary.LittleEndian, _MINORVERSION)
	binary.Write(buf, binary.LittleEndian, _MICROVERSION)

	tr.Set(dl.rootNode.Sub([]byte("version")), buf.Bytes())
}

func (dl directoryLayer) contentsOfNode(node subspace.Subspace, path []string, layer []byte) (DirectorySubspace, error) {
	p, err := dl.nodeSS.Unpack(node)
	if err != nil {
		return nil, err
	}
	prefix := p[0]

	newPath := make([]string, len(dl.path)+len(path))
	copy(newPath, dl.path)
	copy(newPath[len(dl.path):], path)

	pb := prefix.([]byte)
	ss := subspace.FromBytes(pb)

	if bytes.Compare(layer, []byte("partition")) == 0 {
		nssb := make([]byte, len(pb)+1)
		copy(nssb, pb)
		nssb[len(pb)] = 0xFE
		ndl := NewDirectoryLayer(subspace.FromBytes(nssb), ss, false).(directoryLayer)
		ndl.path = newPath
		return directoryPartition{ndl, dl}, nil
	}
	return directorySubspace{ss, dl, newPath, layer}, nil
}

func (dl directoryLayer) nodeWithPrefix(prefix []byte) subspace.Subspace {
	if prefix == nil {
		return nil
	}
	return dl.nodeSS.Sub(prefix)
}

func (dl directoryLayer) find(rtr fdb.ReadTransaction, path []string) *node {
	n := &node{dl.rootNode, []string{}, path, nil}
	for i := range path {
		n = &node{dl.nodeWithPrefix(rtr.Get(n.subspace.Sub(_SUBDIRS, path[i])).MustGet()), path[:i+1], path, nil}
		if !n.exists() || bytes.Compare(n.layer(rtr).MustGet(), []byte("partition")) == 0 {
			return n
		}
	}
	return n
}

func (dl directoryLayer) partitionSubpath(lpath, rpath []string) []string {
	r := make([]string, len(lpath)-len(dl.path)+len(rpath))
	copy(r, lpath[len(dl.path):])
	copy(r[len(lpath)-len(dl.path):], rpath)
	return r
}

func isRangeEmpty(rtr fdb.ReadTransaction, r fdb.Range) bool {
	kvs := rtr.GetRange(r, fdb.RangeOptions{Limit: 1}).GetSliceOrPanic()

	return len(kvs) == 0
}
