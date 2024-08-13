/*
 * mock_kms.go
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

// FoundationDB Mock KMS (Key Management Solution/Service) interface
// Interface runs an HTTP server handling REST calls simulating FDB communications
// with an external KMS.

package main

// Importing module enable .note.gnu.build-id insertion in the ELF executable.
// Few drawbacks of the scheme are:
// 1. Potentially slower builds
// 2. No cross-compilation support
// 3. Limited `go tools` availability.
// TODO: Replace with a better scheme if possible.

import "C"

import (
    "log"
    "math/rand"
    "net/http"
    "sync"
    "time"
)

// KMS supported endpoints
const (
    getEncryptionKeysEndpoint    = "/getEncryptionKeys"
    updateFaultInjectionEndpoint = "/updateFaultInjection"
)

// Routine is responsible to instantiate data-structures necessary for MockKMS functioning
func init () {
    var wg sync.WaitGroup

    wg.Add(2)
    go func(){
        initEncryptCipherMap()
        wg.Done()
    }()
    go func(){
        initFaultLocMap()
        wg.Done()
    }()

    wg.Wait()

	rand.Seed(time.Now().UTC().UnixNano())
}

func main() {
    http.NewServeMux()
    http.HandleFunc(getEncryptionKeysEndpoint, handleGetEncryptionKeys)
    http.HandleFunc(updateFaultInjectionEndpoint, handleUpdateFaultInjection)

    log.Fatal(http.ListenAndServe(":5001", nil))
}