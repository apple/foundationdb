// config_test.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2021 Apple Inc. and the FoundationDB project authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package api

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func loadConfigFromFile(path string) (*ProcessConfiguration, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	config := &ProcessConfiguration{}
	err = decoder.Decode(config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func TestGeneratingArgumentsForDefaultConfig(t *testing.T) {
	config, err := loadConfigFromFile(".testdata/default_config.json")
	if err != nil {
		t.Error(err)
		return
	}

	arguments, err := config.GenerateArguments(1, map[string]string{
		"FDB_PUBLIC_IP":   "10.0.0.1",
		"FDB_POD_IP":      "192.168.0.1",
		"FDB_ZONE_ID":     "zone1",
		"FDB_INSTANCE_ID": "storage-1",
	})
	if err != nil {
		t.Error(err)
		return
	}

	expectedArguments := []string{
		"--cluster-file", ".testdata/fdb.cluster",
		"--public-address", "10.0.0.1:4501", "--listen-address", "192.168.0.1:4501",
		"--datadir", ".testdata/data/1", "--class", "storage",
		"--locality-zoneid", "zone1", "--locality-instance-id", "storage-1",
		"--locality-process-id", "storage-1-1",
	}

	if !reflect.DeepEqual(arguments, expectedArguments) {
		t.Logf("Expected arguments %v, but got arguments %v", expectedArguments, arguments)
		t.Fail()
	}

	config.BinaryPath = "/usr/bin/fdbserver"

	arguments, err = config.GenerateArguments(1, map[string]string{
		"FDB_PUBLIC_IP":   "10.0.0.1",
		"FDB_POD_IP":      "192.168.0.1",
		"FDB_ZONE_ID":     "zone1",
		"FDB_INSTANCE_ID": "storage-1",
	})
	if err != nil {
		t.Error(err)
		return
	}

	expectedArguments = []string{
		"/usr/bin/fdbserver",
		"--cluster-file", ".testdata/fdb.cluster",
		"--public-address", "10.0.0.1:4501", "--listen-address", "192.168.0.1:4501",
		"--datadir", ".testdata/data/1", "--class", "storage",
		"--locality-zoneid", "zone1", "--locality-instance-id", "storage-1",
		"--locality-process-id", "storage-1-1",
	}

	if !reflect.DeepEqual(arguments, expectedArguments) {
		t.Logf("Expected arguments %v, but got arguments %v", expectedArguments, arguments)
		t.Fail()
	}
}

func TestGeneratingArgumentForEnvironmentVariable(t *testing.T) {
	argument := Argument{ArgumentType: EnvironmentArgumentType, Source: "FDB_ZONE_ID"}

	result, err := argument.GenerateArgument(1, map[string]string{"FDB_ZONE_ID": "zone1", "FDB_MACHINE_ID": "machine1"})
	if err != nil {
		t.Error(err)
		return
	}
	if result != "zone1" {
		t.Logf("Expected result zone1, but got result %v", result)
		t.Fail()
		return
	}

	_, err = argument.GenerateArgument(1, map[string]string{"FDB_MACHINE_ID": "machine1"})
	if err == nil {
		t.Logf("Expected error result, but did not get an error")
		t.Fail()
		return
	}
	expectedError := "Missing environment variable FDB_ZONE_ID"
	if err.Error() != expectedError {
		t.Logf("Expected error %s, but got error %s", expectedError, err)
		t.Fail()
		return
	}
}
