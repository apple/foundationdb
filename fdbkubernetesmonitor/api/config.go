// config.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"k8s.io/utils/pointer"
)

// ProcessConfiguration models the configuration for starting a FoundationDB
// process.
type ProcessConfiguration struct {
	// Version provides the version of FoundationDB the process should run.
	Version *Version `json:"version"`

	// RunServers defines whether we should run the server processes.
	// This defaults to true, but you can set it to false to prevent starting
	// new fdbserver processes.
	RunServers *bool `json:"runServers,omitempty"`

	// BinaryPath provides the path to the binary to launch.
	BinaryPath string `json:"-"`

	// Arguments provides the arguments to the process.
	Arguments []Argument `json:"arguments,omitempty"`
}

// Argument defines an argument to the process.
type Argument struct {
	// ArgumentType determines how the value is generated.
	ArgumentType ArgumentType `json:"type,omitempty"`

	// Value provides the value for a Literal type argument.
	Value string `json:"value,omitempty"`

	// Values provides the sub-values for a Concatenate type argument.
	Values []Argument `json:"values,omitempty"`

	// Source provides the name of the environment variable to use for an
	// Environment type argument.
	Source string `json:"source,omitempty"`

	// Multiplier provides a multiplier for the process number for ProcessNumber
	// type arguments.
	Multiplier int `json:"multiplier,omitempty"`

	// Offset provides an offset to add to the process number for ProcessNumber
	// type arguments.
	Offset int `json:"offset,omitempty"`

	// IPFamily provides the family to use for IPList type arguments.
	IPFamily int `json:"ipFamily,omitempty"`
}

// ArgumentType defines the types for arguments.
type ArgumentType string

const (
	// LiteralArgumentType defines an argument with a literal string value.
	LiteralArgumentType ArgumentType = "Literal"

	// ConcatenateArgumentType defines an argument composed of other arguments.
	ConcatenateArgumentType = "Concatenate"

	// EnvironmentArgumentType defines an argument that is pulled from an
	// environment variable.
	EnvironmentArgumentType = "Environment"

	// ProcessNumberArgumentType defines an argument that is calculated using
	// the number of the process in the process list.
	ProcessNumberArgumentType = "ProcessNumber"

	// IPListArgumentType defines an argument that is a comma-separated list of
	// IP addresses, provided through an environment variable.
	IPListArgumentType = "IPList"
)

// GenerateArgument processes an argument and generates its string representation.
func (argument Argument) GenerateArgument(processNumber int, env map[string]string) (string, error) {
	switch argument.ArgumentType {
	case "":
		fallthrough
	case LiteralArgumentType:
		return argument.Value, nil
	case ConcatenateArgumentType:
		concatenated := ""
		for _, childArgument := range argument.Values {
			childValue, err := childArgument.GenerateArgument(processNumber, env)
			if err != nil {
				return "", err
			}
			concatenated += childValue
		}
		return concatenated, nil
	case ProcessNumberArgumentType:
		number := processNumber
		if argument.Multiplier != 0 {
			number = number * argument.Multiplier
		}
		number = number + argument.Offset
		return strconv.Itoa(number), nil
	case EnvironmentArgumentType, IPListArgumentType:
		return argument.LookupEnv(env)
	default:
		return "", fmt.Errorf("unsupported argument type %s", argument.ArgumentType)
	}
}

// LookupEnv looks up the value for an argument from the environment.
func (argument Argument) LookupEnv(env map[string]string) (string, error) {
	var value string
	var present bool
	if env != nil {
		value, present = env[argument.Source]
	}

	if !present {
		value, present = os.LookupEnv(argument.Source)
	}

	if !present {
		return "", fmt.Errorf("missing environment variable %s", argument.Source)
	}

	if argument.ArgumentType == IPListArgumentType {
		ips := strings.Split(value, ",")
		for _, ipString := range ips {
			ip := net.ParseIP(ipString)
			if ip == nil {
				continue
			}
			switch argument.IPFamily {
			case 4:
				if ip.To4() != nil {
					return ipString, nil
				}
			case 6:
				if ip.To16() != nil && ip.To4() == nil {
					return ipString, nil
				}
			default:
				return "", fmt.Errorf("unsupported IP family %d", argument.IPFamily)
			}
		}
		return "", fmt.Errorf("could not find IP with family %d", argument.IPFamily)
	}

	return value, nil
}

// GenerateArguments interprets the arguments in the process configuration and generates a command invocation.
func (configuration *ProcessConfiguration) GenerateArguments(processNumber int, env map[string]string) ([]string, error) {
	results := make([]string, 0, len(configuration.Arguments)+1)
	if configuration.BinaryPath != "" {
		results = append(results, configuration.BinaryPath)
	}
	for _, argument := range configuration.Arguments {
		result, err := argument.GenerateArgument(processNumber, env)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

// ShouldRunServers returns true if RunServers is unset or set to true.
func (configuration *ProcessConfiguration) ShouldRunServers() bool {
	if configuration == nil {
		return false
	}

	return pointer.BoolDeref(configuration.RunServers, true)
}
