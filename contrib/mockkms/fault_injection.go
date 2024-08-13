/*
 * fault_injection.go
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

// Interface supports client to inject fault(s)
// Module enables a client to update { FaultLocation -> FaultStatus } mapping in a
// thread-safe manner, however, client is responsible to synchronize fault status
// updates across 'getEncryptionKeys' REST requests to obtain predictable results.

package main

import (
    "encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
)

type Fault struct {
	Location int		 `json:"fault_location"`
	Enable bool 		 `json:"enable_fault"`
}

type FaultInjectionRequest struct {
	Faults []Fault   	 `json:"faults"`
}

type FaultInjectionResponse struct {
	Faults []Fault   	 `json:"faults"`
}

type faultLocMap struct {
	locMap map[int]bool
	rwLock sync.RWMutex
}

var (
	faultLocMapInstance *faultLocMap		// Singleton mapping of { FaultLocation -> FaultStatus }
)

// Caller is responsible for thread synchronization. Recommended to be invoked during package::init()
func NewFaultLocMap() *faultLocMap {
	if faultLocMapInstance == nil {
		faultLocMapInstance = &faultLocMap{}

		faultLocMapInstance.rwLock = sync.RWMutex{}
		faultLocMapInstance.locMap = map[int]bool {
			READ_HTTP_REQUEST_BODY      : false,
			UNMARSHAL_REQUEST_BODY_JSON : false,
			UNSUPPORTED_QUERY_MODE      : false,
			PARSE_HTTP_REQUEST          : false,
			MARSHAL_RESPONSE            : false,
		}
	}
	return faultLocMapInstance
}

func getLocFaultStatus(loc int) (val bool, found bool) {
	if faultLocMapInstance == nil {
		panic("FaultLocMap not intialized")
		os.Exit(1)
	}

	faultLocMapInstance.rwLock.RLock()
	defer faultLocMapInstance.rwLock.RUnlock()
	val, found = faultLocMapInstance.locMap[loc]
	if !found {
		return
	}
	return
}

func updateLocFaultStatuses(faults []Fault) (updated []Fault, err error) {
	if faultLocMapInstance == nil {
		panic("FaultLocMap not intialized")
		os.Exit(1)
	}

	updated = []Fault{}
	err = nil

	faultLocMapInstance.rwLock.Lock()
	defer faultLocMapInstance.rwLock.Unlock()
	for i := 0; i < len(faults); i++ {
		fault := faults[i]

		oldVal, found := faultLocMapInstance.locMap[fault.Location]
		if !found {
			err = fmt.Errorf("Unknown fault_location '%d'", fault.Location)
			return
		}
		faultLocMapInstance.locMap[fault.Location] = fault.Enable
		log.Printf("Update Location '%d' oldVal '%t' newVal '%t'", fault.Location, oldVal, fault.Enable)
	}

	// return the updated faultLocMap
	for loc, enable := range faultLocMapInstance.locMap {
		var f Fault
		f.Location = loc
		f.Enable = enable
		updated = append(updated, f)
	}
	return
}

func jsonifyFaultArr(w http.ResponseWriter, faults []Fault) (jResp string) {
	resp := FaultInjectionResponse{
		Faults:	faults,
	}

	mResp, err := json.Marshal(resp)
	if err != nil {
		log.Printf("Error marshaling response '%s'", err.Error())
		sendErrorResponse(w, err)
		return
	}
	jResp = string(mResp)
	return
}

func updateFaultLocMap(w http.ResponseWriter, faults []Fault) {
	updated , err := updateLocFaultStatuses(faults)
	if err != nil {
		sendErrorResponse(w, err)
		return
	}

	fmt.Fprintf(w, jsonifyFaultArr(w, updated))
}

func shouldInjectFault(loc int) bool {
	status, found := getLocFaultStatus(loc)
	if !found {
		log.Printf("Unknown fault_location '%d'", loc)
		return false
	}
	return status
}

func handleUpdateFaultInjection(w http.ResponseWriter, r *http.Request) {
	byteArr, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Http request body read error '%s'", err.Error())
		sendErrorResponse(w, err)
		return
	}

	req := FaultInjectionRequest{}
	err = json.Unmarshal(byteArr, &req)
	if err != nil {
		log.Printf("Error parsing FaultInjectionRequest '%s'", string(byteArr))
		sendErrorResponse(w, err)
	}
	updateFaultLocMap(w, req.Faults)
}

func initFaultLocMap() {
	faultLocMapInstance = NewFaultLocMap()
    log.Printf("FaultLocMap int done")
}