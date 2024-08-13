/*
 * mockkms_test.go
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

// MockKMS unit tests, the coverage includes:
// 1. Mock HttpRequest creation and HttpResponse writer.
// 2. Construct fake request to validate the following scenarios:
//  2.1. Request with "unsupported query mode"
//  2.2. Get encryption keys by KeyIds; with and without 'RefreshKmsUrls' flag.
//  2.2. Get encryption keys by DomainIds; with and without 'RefreshKmsUrls' flag.
//  2.3. Random fault injection and response validation

package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	ByKeyIdReqWithRefreshUrls = `{
		"query_mode": "lookupByKeyId",
		"cipher_key_details": [
			{
				"base_cipher_id": 77,
				"encrypt_domain_id": 76
			},
			{
				"base_cipher_id": 2,
				"encrypt_domain_id": -1
			}
		],
		"validation_tokens": [
			{
				"token_name": "1",
				"token_value":"12344"
			},
			{
				"token_name": "2",
				"token_value":"12334"
			}
		],
		"refresh_kms_urls": true
	}`
	ByKeyIdReqWithoutRefreshUrls = `{
		"query_mode": "lookupByKeyId",
		"cipher_key_details": [
			{
				"base_cipher_id": 77,
				"encrypt_domain_id": 76
			},
			{
				"base_cipher_id": 2,
				"encrypt_domain_id": -1
			}
		],
		"validation_tokens": [
			{
				"token_name": "1",
				"token_value":"12344"
			},
			{
				"token_name": "2",
				"token_value":"12334"
			}
		],
		"refresh_kms_urls": false
	}`
	ByDomainIdReqWithRefreshUrls = `{
		"query_mode": "lookupByDomainId",
		"cipher_key_details": [
			{
				"encrypt_domain_id": 76
			},
			{
				"encrypt_domain_id": -1
			}
		],
		"validation_tokens": [
			{
				"token_name": "1",
				"token_value":"12344"
			},
			{
				"token_name": "2",
				"token_value":"12334"
			}
		],
		"refresh_kms_urls": true
	}`
	ByDomainIdReqWithoutRefreshUrls = `{
		"query_mode": "lookupByDomainId",
		"cipher_key_details": [
			{
				"encrypt_domain_id": 76
			},
			{
				"encrypt_domain_id": -1
			}
		],
		"validation_tokens": [
			{
				"token_name": "1",
				"token_value":"12344"
			},
			{
				"token_name": "2",
				"token_value":"12334"
			}
		],
		"refresh_kms_urls": false
	}`
	UnsupportedQueryMode= `{
		"query_mode": "foo_mode",
		"cipher_key_details": [
			{
				"encrypt_domain_id": 76
			},
			{
				"encrypt_domain_id": -1
			}
		],
		"validation_tokens": [
			{
				"token_name": "1",
				"token_value":"12344"
			},
			{
				"token_name": "2",
				"token_value":"12334"
			}
		],
		"refresh_kms_urls": false
	}`
)

func unmarshalValidResponse(data []byte, t *testing.T) (resp GetEncryptKeysResponse) {
	resp = GetEncryptKeysResponse{}
	err := json.Unmarshal(data, &resp)
	if err != nil {
		t.Errorf("Error unmarshaling valid response '%s' error '%v'", string(data), err)
		t.Fail()
	}
	return
}

func unmarshalErrorResponse(data []byte, t *testing.T) (resp ErrorResponse) {
	resp = ErrorResponse{}
	err := json.Unmarshal(data, &resp)
	if err != nil {
		t.Errorf("Error unmarshaling error response resp '%s' error '%v'", string(data), err)
		t.Fail()
	}
	return
}

func checkGetEncyptKeysResponseValidity(resp GetEncryptKeysResponse, t *testing.T) {
	if len(resp.CipherDetails) != 2 {
		t.Errorf("Unexpected CipherDetails count, expected '%d' actual '%d'", 2, len(resp.CipherDetails))
		t.Fail()
	}

	baseCipherIds := [...]uint64 {uint64(77), uint64(2)}
	encryptDomainIds := [...]int64 {int64(76), int64(-1)}

	for i := 0; i < len(resp.CipherDetails); i++ {
		if resp.CipherDetails[i].BaseCipherId != baseCipherIds[i] {
			t.Errorf("Mismatch BaseCipherId, expected '%d' actual '%d'", baseCipherIds[i], resp.CipherDetails[i].BaseCipherId)
			t.Fail()
		}
		if resp.CipherDetails[i].EncryptDomainId != encryptDomainIds[i] {
			t.Errorf("Mismatch EncryptDomainId, expected '%d' actual '%d'", encryptDomainIds[i], resp.CipherDetails[i].EncryptDomainId)
			t.Fail()
		}
		if len(resp.CipherDetails[i].BaseCipher) == 0 {
			t.Error("Empty BaseCipher")
			t.Fail()
		}
	}
}

func runQueryExpectingErrorResponse(payload string, url string, errSubStr string, t *testing.T) {
	body := strings.NewReader(payload)
	req := httptest.NewRequest(http.MethodPost, url, body)
	w := httptest.NewRecorder()
	handleGetEncryptionKeys(w, req)
	res := w.Result()
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Errorf("Error %v", err)
	}

	resp := unmarshalErrorResponse(data, t)
	if !strings.Contains(resp.Err.Detail, errSubStr) {
		t.Errorf("Unexpected error response '%s'", resp.Err.Detail)
		t.Fail()
	}
}

func runQueryExpectingValidResponse(payload string, url string, t *testing.T) {
	body := strings.NewReader(payload)
	req := httptest.NewRequest(http.MethodPost, url, body)
	w := httptest.NewRecorder()
	handleGetEncryptionKeys(w, req)
	res := w.Result()
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Errorf("Error %v", err)
	}

	resp := unmarshalValidResponse(data, t)
	checkGetEncyptKeysResponseValidity(resp, t)
}

func TestUnsupportedQueryMode(t *testing.T) {
	runQueryExpectingErrorResponse(UnsupportedQueryMode, getEncryptionKeysEndpoint, errStrMap()(UNSUPPORTED_QUERY_MODE), t)
}

func TestGetEncryptionKeysByKeyIdsWithRefreshUrls(t *testing.T) {
	runQueryExpectingValidResponse(ByKeyIdReqWithRefreshUrls, getEncryptionKeysEndpoint, t)
}

func TestGetEncryptionKeysByKeyIdsWithoutRefreshUrls(t *testing.T) {
	runQueryExpectingValidResponse(ByKeyIdReqWithoutRefreshUrls, getEncryptionKeysEndpoint, t)
}

func TestGetEncryptionKeysByDomainIdsWithRefreshUrls(t *testing.T) {
	runQueryExpectingValidResponse(ByDomainIdReqWithRefreshUrls, getEncryptionKeysEndpoint, t)
}

func TestGetEncryptionKeysByDomainIdsWithoutRefreshUrls(t *testing.T) {
	runQueryExpectingValidResponse(ByDomainIdReqWithoutRefreshUrls, getEncryptionKeysEndpoint, t)
}

func TestFaultInjection(t *testing.T) {
	numIterations := rand.Intn(701) + 86

	for i := 0; i < numIterations; i++ {
		loc := rand.Intn(MARSHAL_RESPONSE + 1)
		f := Fault{}
		f.Location = loc
		f.Enable = true

		var faults []Fault
		faults = append(faults, f)
		fW := httptest.NewRecorder()
		body := strings.NewReader(jsonifyFaultArr(fW, faults))
		fReq := httptest.NewRequest(http.MethodPost, updateFaultInjectionEndpoint, body)
		handleUpdateFaultInjection(fW, fReq)
		if !shouldInjectFault(loc) {
			t.Errorf("Expected fault enabled for loc '%d'", loc)
			t.Fail()
		}

		var payload string
		lottery := rand.Intn(100)
		if lottery < 25 {
			payload = ByKeyIdReqWithRefreshUrls
		} else if lottery >= 25 && lottery < 50 {
			payload = ByKeyIdReqWithoutRefreshUrls
		} else if lottery >= 50 && lottery < 75 {
			payload = ByDomainIdReqWithRefreshUrls
		} else {
			payload = ByDomainIdReqWithoutRefreshUrls
		}
		runQueryExpectingErrorResponse(payload, getEncryptionKeysEndpoint, errStrMap()(loc), t)

		// reset Fault
		faults[0].Enable = false
		fW = httptest.NewRecorder()
		body = strings.NewReader(jsonifyFaultArr(fW, faults))
		fReq = httptest.NewRequest(http.MethodPost, updateFaultInjectionEndpoint, body)
		handleUpdateFaultInjection(fW, fReq)
		if shouldInjectFault(loc) {
			t.Errorf("Expected fault disabled for loc '%d'", loc)
			t.Fail()
		}
	}
}