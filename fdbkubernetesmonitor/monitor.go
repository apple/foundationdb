// monitor.go
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

package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// errorBackoffSeconds is the time to wait after a process fails before starting
// another process.
const errorBackoffSeconds = 5

// Monitor provides the main monitor loop
type Monitor struct {
	// ConfigFile defines the path to the config file to load.
	ConfigFile string

	// FDBServerPath defines the path to the fdbserver binary.
	FDBServerPath string

	// ActiveConfiguration defines the active process configuration.
	ActiveConfiguration *ProcessConfiguration

	// ActiveConfigurationBytes defines the source data for the active process
	// configuration.
	ActiveConfigurationBytes []byte

	// ProcessIDs stores the PIDs of the processes that are running. A PID of
	// zero will indicate that a process does not have a run loop. A PID of -1
	// will indicate that a process has a run loop but is not currently running
	// the subprocess.
	ProcessesIDs []int

	// Mutex defines a mutex around working with configuration.
	Mutex sync.Mutex
}

// StartMonitor starts the monitor loop.
func StartMonitor(configFile string, fdbserverPath string) {
	monitor := &Monitor{ConfigFile: configFile, FDBServerPath: fdbserverPath}
	monitor.Run()
}

// LoadConfiguration loads the latest configuration from the config file.
func (monitor *Monitor) LoadConfiguration() {
	file, err := os.Open(monitor.ConfigFile)
	if err != nil {
		log.Print(err.Error())
		return
	}
	defer file.Close()
	configuration := &ProcessConfiguration{}
	configurationBytes, err := io.ReadAll(file)
	if err != nil {
		log.Print(err.Error())
	}
	err = json.Unmarshal(configurationBytes, configuration)
	if err != nil {
		log.Print(err)
		return
	}

	_, err = configuration.GenerateArguments(1, nil)
	if err != nil {
		log.Print(err)
		return
	}

	log.Printf("Received new configuration file")
	monitor.Mutex.Lock()
	defer monitor.Mutex.Unlock()

	if configuration.ServerCount == 0 {
		configuration.ServerCount = 1
	}

	if monitor.ProcessesIDs == nil {
		monitor.ProcessesIDs = make([]int, configuration.ServerCount+1)
	} else {
		for len(monitor.ProcessesIDs) <= configuration.ServerCount {
			monitor.ProcessesIDs = append(monitor.ProcessesIDs, 0)
		}
	}

	monitor.ActiveConfiguration = configuration
	monitor.ActiveConfigurationBytes = configurationBytes

	for processNumber := 1; processNumber <= configuration.ServerCount; processNumber++ {
		if monitor.ProcessesIDs[processNumber] == 0 {
			monitor.ProcessesIDs[processNumber] = -1
			tempNumber := processNumber
			go func() { monitor.RunProcess(tempNumber) }()
		}
	}
}

// RunProcess runs a loop to continually start and watch a process.
func (monitor *Monitor) RunProcess(processNumber int) {
	log.Printf("Starting run loop for subprocess %d", processNumber)
	for {
		arguments, err := monitor.ActiveConfiguration.GenerateArguments(processNumber, nil)
		arguments = append([]string{monitor.FDBServerPath}, arguments...)
		if err != nil {
			log.Print(err)
			time.Sleep(errorBackoffSeconds * time.Second)
		}
		cmd := exec.Cmd{
			Path:   arguments[0],
			Args:   arguments,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		}

		log.Printf("Starting subprocess #%d: %v", processNumber, arguments)
		err = cmd.Start()
		if err != nil {
			log.Printf("Error from subprocess %d: %s", processNumber, err.Error())
			log.Printf("Subprocess #%d will restart in %d seconds", processNumber, errorBackoffSeconds)
			time.Sleep(errorBackoffSeconds * time.Second)
			continue
		}

		monitor.Mutex.Lock()
		monitor.ProcessesIDs[processNumber] = cmd.Process.Pid
		monitor.Mutex.Unlock()

		err = cmd.Wait()
		log.Printf("Subprocess #%d terminated", processNumber)

		if err != nil {
			log.Printf("Error from subprocess #%d: %s", processNumber, err.Error())
		}

		monitor.Mutex.Lock()
		monitor.ProcessesIDs[processNumber] = -1
		if monitor.ActiveConfiguration.ServerCount < processNumber {
			log.Printf("Terminating run loop for subprocess %d", processNumber)
			monitor.ProcessesIDs[processNumber] = 0
			monitor.Mutex.Unlock()
			return
		}
		monitor.Mutex.Unlock()

		log.Printf("Subprocess #%d will restart in %d seconds", processNumber, errorBackoffSeconds)
		time.Sleep(errorBackoffSeconds * time.Second)
	}
}

// WatchConfiguration detects changes to the monitor configuration file.
func (monitor *Monitor) WatchConfiguration(watcher *fsnotify.Watcher) {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			log.Printf("Detected event on monitor conf file: %v", event)
			if event.Op&fsnotify.Write == fsnotify.Write {
				monitor.LoadConfiguration()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Print(err)
		}
	}
}

// Run runs the monitor loop.
func (monitor *Monitor) Run() {
	done := make(chan bool, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		latestSignal := <-signals
		log.Printf("Received signal %v", latestSignal)
		for processNumber, processID := range monitor.ProcessesIDs {
			if processID > 0 {
				process, err := os.FindProcess(processID)
				if err != nil {
					log.Printf("Error finding subprocess #%d (PID %d): %s", processNumber, processID, err.Error())
					continue
				}
				log.Printf("Sending signal %v to subprocess #%d (PID %d)", latestSignal, processNumber, processID)
				err = process.Signal(latestSignal)
				if err != nil {
					log.Printf("Error signaling subprocess #%d (PID %d): %s", processNumber, processID, err.Error())
					continue
				}
			}
		}
		done <- true
	}()

	monitor.LoadConfiguration()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	err = watcher.Add(monitor.ConfigFile)
	if err != nil {
		panic(err)
	}

	defer watcher.Close()
	go func() { monitor.WatchConfiguration(watcher) }()

	<-done
}
