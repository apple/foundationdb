/*
 * database.go
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

func Transact(db Database, f func(t Transaction) error) error {
	_, err := db.Transact(func(t Transaction) (interface{}, error) {
		return nil, f(t)
	})
	return err
}

func Transact1[T any](db Database, f func(t Transaction) (T, error)) (T, error) {
	v, err := db.Transact(func(t Transaction) (interface{}, error) {
		return f(t)
	})
	if v == nil {
		var empty T
		return empty, err
	}
	return v.(T), nil
}

func Transact2[T1 any, T2 any](db Database, f func(t Transaction) (T1, T2, error)) (T1, T2, error) {
	v, err := db.Transact(func(t Transaction) (interface{}, error) {
		t1, t2, err := f(t)
		return []any{t1, t2}, err
	})
	if v == nil {
		var t1 T1
		var t2 T2
		return t1, t2, err
	}
	a := v.([]interface{})
	return a[0].(T1), a[1].(T2), err
}

func Transact3[T1 any, T2 any, T3 any](db Database, f func(t Transaction) (T1, T2, T3, error)) (T1, T2, T3, error) {
	v, err := db.Transact(func(t Transaction) (interface{}, error) {
		t1, t2, t3, err := f(t)
		return []any{t1, t2, t3}, err
	})
	if v == nil {
		var t1 T1
		var t2 T2
		var t3 T3
		return t1, t2, t3, err
	}
	a := v.([]interface{})
	return a[0].(T1), a[1].(T2), a[2].(T3), err
}

func ReadTransact(db Database, f func(t ReadTransaction) error) error {
	_, err := db.ReadTransact(func(t ReadTransaction) (interface{}, error) {
		return nil, f(t)
	})
	return err
}

func ReadTransact1[T any](db Database, f func(t ReadTransaction) (T, error)) (T, error) {
	v, err := db.ReadTransact(func(t ReadTransaction) (interface{}, error) {
		return f(t)
	})
	if v == nil {
		var empty T
		return empty, err
	}
	return v.(T), nil
}

func ReadTransact2[T1 any, T2 any](db Database, f func(t ReadTransaction) (T1, T2, error)) (T1, T2, error) {
	v, err := db.ReadTransact(func(t ReadTransaction) (interface{}, error) {
		t1, t2, err := f(t)
		return []any{t1, t2}, err
	})
	if v == nil {
		var t1 T1
		var t2 T2
		return t1, t2, err
	}
	a := v.([]interface{})
	return a[0].(T1), a[1].(T2), err
}

func ReadTransact3[T1 any, T2 any, T3 any](db Database, f func(t ReadTransaction) (T1, T2, T3, error)) (T1, T2, T3, error) {
	v, err := db.ReadTransact(func(t ReadTransaction) (interface{}, error) {
		t1, t2, t3, err := f(t)
		return []any{t1, t2, t3}, err
	})
	if v == nil {
		var t1 T1
		var t2 T2
		var t3 T3
		return t1, t2, t3, err
	}
	a := v.([]interface{})
	return a[0].(T1), a[1].(T2), a[2].(T3), err
}
