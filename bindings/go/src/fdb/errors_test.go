/*
 * errors_test.go
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
	"errors"
	"fmt"
	"testing"
)

const API_VERSION int = 800

func TestErrorWrapping(t *testing.T) {
	MustAPIVersion(API_VERSION)
	db := MustOpenDefault()

	testCases := []error{
		nil,
		&Error{
			Code: 2007,
		},
		fmt.Errorf("wrapped error: %w", &Error{
			Code: 2007,
		}),
		Error{
			Code: 2007,
		},
		fmt.Errorf("wrapped error: %w", Error{
			Code: 2007,
		}),
		errors.New("custom error"),
	}

	for _, inputError := range testCases {
		_, outputError := db.ReadTransact(func(rtr ReadTransaction) (interface{}, error) {
			return nil, inputError
		})
		if inputError != outputError {
			t.Errorf("expected error %v to be the same as %v", outputError, inputError)
		}
	}
}
