/*
 * errors.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

//go:generate go run ./internal/gen_errors/main.go -in ../../../flow/include/flow/error_definitions.h -out fdb/error_codes_generated.go

package fdb

// #define FDB_API_VERSION 800
// #include <foundationdb/fdb_c.h>
import "C"

import (
	"fmt"
)

// Error represents a low-level error returned by the FoundationDB C library. An
// Error may be returned by any FoundationDB API function that returns error, or
// as a panic from any FoundationDB API function whose name ends with OrPanic.
//
// Use errors.Is with the sentinel variables defined in error_codes_generated.go
// to check for specific error codes:
//
//	errors.Is(err, fdb.ErrTransactionTooOld)
//
// When using (Database).Transact, non-fatal errors will be retried automatically.
type Error struct {
	Code int
}

func (e Error) Error() string {
	return fmt.Sprintf("FoundationDB error code %d (%s)", e.Code, C.GoString(C.fdb_get_error(C.fdb_error_t(e.Code))))
}

// Is implements the errors.Is contract. It matches target if target is an Error
// or *Error with the same Code, enabling errors.Is(err, fdb.ErrTransactionTooOld).
func (e Error) Is(target error) bool {
	switch t := target.(type) {
	case Error:
		return e.Code == t.Code
	case *Error:
		return t != nil && e.Code == t.Code
	default:
		return false
	}
}
