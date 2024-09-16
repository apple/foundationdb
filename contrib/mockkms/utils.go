/*
 * utils.go
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
    "encoding/json"
	"fmt"
	"log"
	"net/http"
)

type ErrorDetail struct {
    Detail string                          `json:"details"`
}

type ErrorResponse struct {
    Err ErrorDetail                        `json:"error"`
}

func sendErrorResponse(w http.ResponseWriter, err error) {
    e := ErrorDetail{}
    e.Detail = fmt.Sprintf("Error: %s", err.Error())
    resp := ErrorResponse{
        Err:    e,
    }

    mResp,err  := json.Marshal(resp)
    if err != nil {
        log.Printf("Error marshalling error response %s", err.Error())
        panic(err)
    }
    fmt.Fprintf(w, string(mResp))
}