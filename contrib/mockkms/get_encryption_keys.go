/*
 * get_encryption_keys.go
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

// GetEncryptionKeys handler
// Handler is resposible for the following:
// 1. Parse the incoming HttpRequest and validate JSON request structural sanity
// 2. Ability to handle getEncryptionKeys by 'KeyId' or 'DomainId' as requested
// 3. Ability to inject faults if requested

package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "math/rand"
    "net/http"
)

type CipherDetailRes struct {
    BaseCipherId    uint64 `json:"base_cipher_id"`
    EncryptDomainId int64  `json:"encrypt_domain_id"`
    BaseCipher      string `json:"base_cipher"`
}

type ValidationToken struct {
    TokenName  string `json:"token_name"`
    TokenValue string `json:"token_value"`
}

type CipherDetailReq struct {
    BaseCipherId    uint64 `json:"base_cipher_id"`
    EncryptDomainId int64  `json:"encrypt_domain_id"`
}

type GetEncryptKeysResponse struct {
    CipherDetails []CipherDetailRes `json:"cipher_key_details"`
    KmsUrls       []string          `json:"kms_urls"`
}

type GetEncryptKeysRequest struct {
    QueryMode        string            `json:"query_mode"`
    CipherDetails    []CipherDetailReq `json:"cipher_key_details"`
    ValidationTokens []ValidationToken `json:"validation_tokens"`
    RefreshKmsUrls   bool              `json:"refresh_kms_urls"`
}

type cipherMapInstanceSingleton map[uint64][]byte

const (
    READ_HTTP_REQUEST_BODY = iota
    UNMARSHAL_REQUEST_BODY_JSON
    UNSUPPORTED_QUERY_MODE
    PARSE_HTTP_REQUEST
    MARSHAL_RESPONSE
)

const (
    maxCipherKeys = uint64(1024 * 1024) // Max cipher keys
    maxCipherSize = 16                  // Max cipher buffer size
)

var (
    cipherMapInstance cipherMapInstanceSingleton // Singleton mapping of { baseCipherId -> baseCipher }
)

// const mapping of { Location -> errorString }
func errStrMap() func(int) string {
	_errStrMap := map[int]string{
		READ_HTTP_REQUEST_BODY:      "Http request body read error",
		UNMARSHAL_REQUEST_BODY_JSON: "Http request body unmarshal error",
		UNSUPPORTED_QUERY_MODE:      "Unsupported query_mode",
		PARSE_HTTP_REQUEST:          "Error parsing GetEncryptionKeys request",
		MARSHAL_RESPONSE:            "Error marshaling response",
	}

	return func(key int) string {
		return _errStrMap[key]
	}
}

// Caller is responsible for thread synchronization. Recommended to be invoked during package::init()
func NewCipherMap(maxKeys uint64, cipherSize int) cipherMapInstanceSingleton {
	if cipherMapInstance == nil {
		cipherMapInstance = make(map[uint64][]byte)

		for i := uint64(1); i <= maxKeys; i++ {
			cipher := make([]byte, cipherSize)
			rand.Read(cipher)
			cipherMapInstance[i] = cipher
		}
		log.Printf("KMS cipher map populate done, maxCiphers '%d'", maxCipherKeys)
	}
	return cipherMapInstance
}

func getKmsUrls() (urls []string) {
	urlCount := rand.Intn(5) + 1
	for i := 1; i <= urlCount; i++ {
		url := fmt.Sprintf("https://KMS/%d:%d:%d:%d", i, i, i, i)
		urls = append(urls, url)
	}
	return
}

func isEncryptDomainIdValid(id int64) bool {
	if id > 0 || id == -1 || id == -2 {
		return true
	}
	return false
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func getBaseCipherIdFromDomainId(domainId int64) (baseCipherId uint64) {
	baseCipherId = uint64(1) + uint64(abs(domainId))%maxCipherKeys
	return
}

func getEncryptionKeysByKeyIds(w http.ResponseWriter, byteArr []byte) {
	req := GetEncryptKeysRequest{}
	err := json.Unmarshal(byteArr, &req)
	if err != nil || shouldInjectFault(PARSE_HTTP_REQUEST) {
		var err error
		if shouldInjectFault(PARSE_HTTP_REQUEST) {
			err = fmt.Errorf("[FAULT] %s %s'", errStrMap()(PARSE_HTTP_REQUEST), string(byteArr))
		} else {
			err = fmt.Errorf("%s %s' err '%v'", errStrMap()(PARSE_HTTP_REQUEST), string(byteArr), err)
		}
		log.Println(err.Error())
		sendErrorResponse(w, err)
		return
	}

	var details []CipherDetailRes
	for i := 0; i < len(req.CipherDetails); i++ {
		var baseCipherId = uint64(req.CipherDetails[i].BaseCipherId)

		var encryptDomainId = int64(req.CipherDetails[i].EncryptDomainId)
		if !isEncryptDomainIdValid(encryptDomainId) {
			err := fmt.Errorf("EncryptDomainId not valid '%d'", encryptDomainId)
			sendErrorResponse(w, err)
			return
		}

		cipher, found := cipherMapInstance[baseCipherId]
		if !found {
			err := fmt.Errorf("BaseCipherId not found '%d'", baseCipherId)
			sendErrorResponse(w, err)
			return
		}

		var detail = CipherDetailRes{
			BaseCipherId:    baseCipherId,
			EncryptDomainId: encryptDomainId,
			BaseCipher:      string(cipher),
		}
		details = append(details, detail)
	}

	var urls []string
	if req.RefreshKmsUrls {
		urls = getKmsUrls()
	}

	resp := GetEncryptKeysResponse{
		CipherDetails: details,
		KmsUrls:       urls,
	}

	mResp, err := json.Marshal(resp)
	if err != nil || shouldInjectFault(MARSHAL_RESPONSE) {
		var err error
		if shouldInjectFault(MARSHAL_RESPONSE) {
			err = fmt.Errorf("[FAULT] %s", errStrMap()(MARSHAL_RESPONSE))
		} else {
			err = fmt.Errorf("%s err '%v'", errStrMap()(MARSHAL_RESPONSE), err)
		}
		log.Println(err.Error())
		sendErrorResponse(w, err)
		return
	}

	fmt.Fprintf(w, string(mResp))
}

func getEncryptionKeysByDomainIds(w http.ResponseWriter, byteArr []byte) {
	req := GetEncryptKeysRequest{}
	err := json.Unmarshal(byteArr, &req)
	if err != nil || shouldInjectFault(PARSE_HTTP_REQUEST) {
		var err error
		if shouldInjectFault(PARSE_HTTP_REQUEST) {
			err = fmt.Errorf("[FAULT] %s '%s'", errStrMap()(PARSE_HTTP_REQUEST), string(byteArr))
		} else {
			err = fmt.Errorf("%s '%s' err '%v'", errStrMap()(PARSE_HTTP_REQUEST), string(byteArr), err)
		}
		log.Println(err.Error())
		sendErrorResponse(w, err)
		return
	}

	var details []CipherDetailRes
	for i := 0; i < len(req.CipherDetails); i++ {
		var encryptDomainId = int64(req.CipherDetails[i].EncryptDomainId)
		if !isEncryptDomainIdValid(encryptDomainId) {
			err := fmt.Errorf("EncryptDomainId not valid '%d'", encryptDomainId)
			sendErrorResponse(w, err)
			return
		}

		var baseCipherId = getBaseCipherIdFromDomainId(encryptDomainId)
		cipher, found := cipherMapInstance[baseCipherId]
		if !found {
			err := fmt.Errorf("BaseCipherId not found '%d'", baseCipherId)
			sendErrorResponse(w, err)
			return
		}

		var detail = CipherDetailRes{
			BaseCipherId:    baseCipherId,
			EncryptDomainId: encryptDomainId,
			BaseCipher:      string(cipher),
		}
		details = append(details, detail)
	}

	var urls []string
	if req.RefreshKmsUrls {
		urls = getKmsUrls()
	}

	resp := GetEncryptKeysResponse{
		CipherDetails: details,
		KmsUrls:       urls,
	}

	mResp, err := json.Marshal(resp)
	if err != nil || shouldInjectFault(MARSHAL_RESPONSE) {
		var err error
		if shouldInjectFault(MARSHAL_RESPONSE) {
			err = fmt.Errorf("[FAULT] %s", errStrMap()(MARSHAL_RESPONSE))
		} else {
			err = fmt.Errorf("%s err '%v'", errStrMap()(MARSHAL_RESPONSE), err)
		}
		log.Println(err.Error())
		sendErrorResponse(w, err)
		return
	}

	fmt.Fprintf(w, string(mResp))
}

func handleGetEncryptionKeys(w http.ResponseWriter, r *http.Request) {
	byteArr, err := ioutil.ReadAll(r.Body)
	if err != nil || shouldInjectFault(READ_HTTP_REQUEST_BODY) {
		var err error
		if shouldInjectFault(READ_HTTP_REQUEST_BODY) {
			err = fmt.Errorf("[FAULT] %s", errStrMap()(READ_HTTP_REQUEST_BODY))
		} else {
			err = fmt.Errorf("%s err '%v'", errStrMap()(READ_HTTP_REQUEST_BODY), err)
		}
		log.Println(err.Error())
		sendErrorResponse(w, err)
		return
	}

	var arbitrary_json map[string]interface{}
	err = json.Unmarshal(byteArr, &arbitrary_json)
	if err != nil || shouldInjectFault(UNMARSHAL_REQUEST_BODY_JSON) {
		var err error
		if shouldInjectFault(UNMARSHAL_REQUEST_BODY_JSON) {
			err = fmt.Errorf("[FAULT] %s", errStrMap()(UNMARSHAL_REQUEST_BODY_JSON))
		} else {
			err = fmt.Errorf("%s err '%v'", errStrMap()(UNMARSHAL_REQUEST_BODY_JSON), err)
		}
		log.Println(err.Error())
		sendErrorResponse(w, err)
		return
	}

	if shouldInjectFault(UNSUPPORTED_QUERY_MODE) {
		err = fmt.Errorf("[FAULT] %s '%s'", errStrMap()(UNSUPPORTED_QUERY_MODE), arbitrary_json["query_mode"])
		sendErrorResponse(w, err)
		return
	} else if arbitrary_json["query_mode"] == "lookupByKeyId" {
		getEncryptionKeysByKeyIds(w, byteArr)
	} else if arbitrary_json["query_mode"] == "lookupByDomainId" {
		getEncryptionKeysByDomainIds(w, byteArr)
	} else {
		err = fmt.Errorf("%s '%s'", errStrMap()(UNSUPPORTED_QUERY_MODE), arbitrary_json["query_mode"])
		sendErrorResponse(w, err)
		return
	}
}

func initEncryptCipherMap() {
	cipherMapInstance = NewCipherMap(maxCipherKeys, maxCipherSize)
}
