package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"os"
	"sort"
	"strconv"
)

// TraceEvent represents a single event from the trace file.
// FDB trace events have variable attributes depending on the event type,
// so we store them as a map of key-value pairs.
type TraceEvent struct {
	Severity string
	Time     string
	DateTime string
	Type     string
	Machine  string
	ID       string

	// Parsed time as float for easy comparison
	TimeValue float64

	// Additional attributes specific to event type
	Attrs map[string]string
}

// DBConfig represents the database configuration
type DBConfig struct {
	Time                  float64
	RedundancyMode        string `json:"redundancy_mode"`
	UsableRegions         int    `json:"usable_regions"`
	Logs                  int    `json:"logs"`
	LogRouters            int    `json:"log_routers"`
	RemoteLogs            int    `json:"remote_logs"`
	Proxies               int    `json:"proxies"`
	GrvProxies            int    `json:"grv_proxies"`
	BackupWorkerEnabled   int    `json:"backup_worker_enabled"`
	StorageEngine         string `json:"storage_engine"`
	RemoteRedundancyMode  string `json:"remote_redundancy_mode"`
	TenantMode            string `json:"tenant_mode"`
	// Add other fields as needed
	RawJSON map[string]interface{} // Full JSON for reference
}

// RecoveryState represents a MasterRecoveryState event
type RecoveryState struct {
	Time       float64
	StatusCode string
	Status     string
	EventIndex int // Index of this event in the Events slice
}

// EpochVersionInfo tracks version information per epoch/generation
// Populated from FinalRecoveryVersionInfo (KCV, RV) and UpdateRegistration (epoch, recoveryTxnVersion)
type EpochVersionInfo struct {
	Time                  float64 // Time when this info was fully populated
	Epoch                 int64   // RecoveryCount from UpdateRegistration
	KCV                   int64   // Known committed version from FinalRecoveryVersionInfo
	RV                    int64   // Recovery version (MinEnd) from FinalRecoveryVersionInfo
	RecoveryTxnVersion    int64   // Recovery transaction version from UpdateRegistration
	LastEpochEnd          int64   // LastEpochEnd from UpdateRegistration (should match RV)
	EventIndex            int     // Index of the event that completed this info
	HasKCV                bool    // Whether KCV/RV has been populated
	HasRecoveryTxnVersion bool    // Whether epoch/recoveryTxnVersion has been populated
}

// TraceData holds the parsed trace file and provides time-based access
type TraceData struct {
	Events         []TraceEvent
	Configs        []DBConfig // Database configurations over time
	RecoveryStates []RecoveryState
	EpochVersions  []EpochVersionInfo // Epoch version info over time
	MinTime        float64
	MaxTime        float64
	TimeStep       float64 // Default time increment for scrubbing
}

// parseTraceFile reads an XML trace file and returns TraceData.
func parseTraceFile(filepath string) (*TraceData, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open trace file: %w", err)
	}
	defer file.Close()

	// Get file size to estimate event count for pre-allocation
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	// Estimate ~400 bytes per event on average, pre-allocate to reduce reallocations
	estimatedEvents := int(fileSize / 400)
	if estimatedEvents < 1000 {
		estimatedEvents = 1000
	}

	decoder := xml.NewDecoder(file)
	events := make([]TraceEvent, 0, estimatedEvents)
	configs := make([]DBConfig, 0, 100) // Pre-allocate for configs
	minTime := 0.0
	maxTime := 0.0

	fmt.Printf("Loading trace file (%.1f GB)...\n", float64(fileSize)/(1024*1024*1024))

	eventCount := 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to decode XML: %w", err)
		}

		switch elem := token.(type) {
		case xml.StartElement:
			if elem.Name.Local == "Event" {
				event := TraceEvent{
					Attrs: make(map[string]string, 10), // Pre-allocate with capacity
				}

				// Parse all attributes
				for _, attr := range elem.Attr {
					switch attr.Name.Local {
					case "Severity":
						event.Severity = attr.Value
					case "Time":
						event.Time = attr.Value
						// Parse time as float
						if t, err := strconv.ParseFloat(attr.Value, 64); err == nil {
							event.TimeValue = t
							if t > maxTime {
								maxTime = t
							}
						}
					case "DateTime":
						event.DateTime = attr.Value
					case "Type":
						event.Type = attr.Value
					case "Machine":
						event.Machine = attr.Value
					case "ID":
						event.ID = attr.Value
					default:
						// Store any additional attributes
						event.Attrs[attr.Name.Local] = attr.Value
					}
				}

				events = append(events, event)
				eventCount++

				// Show progress every 100k events
				if eventCount%100000 == 0 {
					fmt.Printf("  Loaded %dK events...\n", eventCount/1000)
				}

				// Parse DB config if this is a MasterRecoveryState event
				if event.Type == "MasterRecoveryState" {
					if confStr, ok := event.Attrs["Conf"]; ok {
						if config := parseDBConfig(confStr, event.TimeValue); config != nil {
							configs = append(configs, *config)
						}
					}
				}
			}
		}
	}

	fmt.Printf("Loaded %d events total. Sorting...\n", len(events))

	// Sort events by time
	sort.Slice(events, func(i, j int) bool {
		return events[i].TimeValue < events[j].TimeValue
	})

	// Sort configs by time
	sort.Slice(configs, func(i, j int) bool {
		return configs[i].Time < configs[j].Time
	})

	fmt.Println("Building indices...")

	// Build RecoveryStates array from sorted events with correct indices
	recoveryStates := make([]RecoveryState, 0, len(configs))
	for i, event := range events {
		if event.Type == "MasterRecoveryState" {
			statusCode := event.Attrs["StatusCode"]
			status := event.Attrs["Status"]
			if statusCode != "" && status != "" {
				recoveryStates = append(recoveryStates, RecoveryState{
					Time:       event.TimeValue,
					StatusCode: statusCode,
					Status:     status,
					EventIndex: i,
				})
			}
		}
	}

	// Build EpochVersions array from GetDurableResult and UpdateRegistration events
	// GetDurableResult provides KnownCommittedVersion (KCV) and EpochEnd (RV)
	// UpdateRegistration provides RecoveryCount (epoch), RecoveryTxnVersion, LastEpochEnd (RV)
	epochVersions := make([]EpochVersionInfo, 0, 100)

	// Track KCV from GetDurableResult keyed by EndVersion (which equals LastEpochEnd in UpdateRegistration)
	kcvByRV := make(map[int64]int64) // EndVersion/LastEpochEnd -> KCV

	// First pass: collect KCV values from GetDurableResult
	for _, event := range events {
		if event.Type == "GetDurableResult" {
			var endVersion, kcv int64
			// Use EndVersion (not EpochEnd which can be INT64_MAX placeholder)
			if endVersionStr, ok := event.Attrs["EndVersion"]; ok {
				endVersion, _ = strconv.ParseInt(endVersionStr, 10, 64)
			}
			if kcvStr, ok := event.Attrs["KnownCommittedVersion"]; ok {
				kcv, _ = strconv.ParseInt(kcvStr, 10, 64)
			}
			if endVersion > 0 && kcv > 0 {
				kcvByRV[endVersion] = kcv
			}
		}
	}

	// Second pass: build epoch versions from UpdateRegistration
	for i, event := range events {
		if event.Type == "UpdateRegistration" {
			var epoch, recoveryTxn, lastEpochEnd int64

			if epochStr, ok := event.Attrs["RecoveryCount"]; ok {
				epoch, _ = strconv.ParseInt(epochStr, 10, 64)
			}
			if recoveryTxnStr, ok := event.Attrs["RecoveryTxnVersion"]; ok {
				recoveryTxn, _ = strconv.ParseInt(recoveryTxnStr, 10, 64)
			}
			if lastEpochEndStr, ok := event.Attrs["LastEpochEnd"]; ok {
				lastEpochEnd, _ = strconv.ParseInt(lastEpochEndStr, 10, 64)
			}

			// Skip events with invalid/unset values (-1 means not determined yet)
			if recoveryTxn < 0 || lastEpochEnd < 0 {
				continue
			}

			// Only record if we have meaningful data
			if epoch > 0 || recoveryTxn > 0 || lastEpochEnd > 0 {
				info := EpochVersionInfo{
					Time:               event.TimeValue,
					Epoch:              epoch,
					RV:                 lastEpochEnd,
					RecoveryTxnVersion: recoveryTxn,
					LastEpochEnd:       lastEpochEnd,
					EventIndex:         i,
				}

				// Try to find matching KCV from GetDurableResult
				if kcv, ok := kcvByRV[lastEpochEnd]; ok {
					info.KCV = kcv
					info.HasKCV = true
				}
				info.HasRecoveryTxnVersion = true

				epochVersions = append(epochVersions, info)
			}
		}
	}

	// Sort epoch versions by time
	sort.Slice(epochVersions, func(i, j int) bool {
		return epochVersions[i].Time < epochVersions[j].Time
	})

	// Calculate minimum time step from actual event intervals (sample to avoid full scan)
	timeStep := 0.1 // Default fallback
	if len(events) > 1 {
		// Only sample first 10000 events to find min diff (faster)
		sampleSize := 10000
		if len(events) < sampleSize {
			sampleSize = len(events)
		}
		minDiff := events[1].TimeValue - events[0].TimeValue
		for i := 2; i < sampleSize; i++ {
			diff := events[i].TimeValue - events[i-1].TimeValue
			if diff > 0 && diff < minDiff {
				minDiff = diff
			}
		}
		if minDiff > 0 {
			timeStep = minDiff
		}
	}

	fmt.Println("Ready!")

	return &TraceData{
		Events:         events,
		Configs:        configs,
		RecoveryStates: recoveryStates,
		EpochVersions:  epochVersions,
		MinTime:        minTime,
		MaxTime:        maxTime,
		TimeStep:       timeStep,
	}, nil
}

// parseDBConfig parses the HTML-encoded JSON config string
func parseDBConfig(confStr string, time float64) *DBConfig {
	// Decode HTML entities (&quot; -> ")
	decoded := html.UnescapeString(confStr)

	// Parse JSON
	var rawConfig map[string]interface{}
	if err := json.Unmarshal([]byte(decoded), &rawConfig); err != nil {
		return nil
	}

	config := &DBConfig{
		Time:    time,
		RawJSON: rawConfig,
	}

	// Extract common fields
	if v, ok := rawConfig["redundancy_mode"].(string); ok {
		config.RedundancyMode = v
	}
	if v, ok := rawConfig["usable_regions"].(float64); ok {
		config.UsableRegions = int(v)
	}
	if v, ok := rawConfig["logs"].(float64); ok {
		config.Logs = int(v)
	}
	if v, ok := rawConfig["log_routers"].(float64); ok {
		config.LogRouters = int(v)
	}
	if v, ok := rawConfig["remote_logs"].(float64); ok {
		config.RemoteLogs = int(v)
	}
	if v, ok := rawConfig["proxies"].(float64); ok {
		config.Proxies = int(v)
	}
	if v, ok := rawConfig["grv_proxies"].(float64); ok {
		config.GrvProxies = int(v)
	}
	if v, ok := rawConfig["backup_worker_enabled"].(float64); ok {
		config.BackupWorkerEnabled = int(v)
	}
	if v, ok := rawConfig["storage_engine"].(string); ok {
		config.StorageEngine = v
	}
	if v, ok := rawConfig["remote_redundancy_mode"].(string); ok {
		config.RemoteRedundancyMode = v
	}
	if v, ok := rawConfig["tenant_mode"].(string); ok {
		config.TenantMode = v
	}

	return config
}

// GetEventsUpToTime returns all events that occurred up to and including the given time
func (td *TraceData) GetEventsUpToTime(targetTime float64) []TraceEvent {
	// Binary search to find the index
	idx := sort.Search(len(td.Events), func(i int) bool {
		return td.Events[i].TimeValue > targetTime
	})

	if idx == 0 {
		return []TraceEvent{}
	}

	return td.Events[:idx]
}

// GetLatestConfigAtTime returns the latest DB config at or before the given time
func (td *TraceData) GetLatestConfigAtTime(targetTime float64) *DBConfig {
	// Binary search to find the latest config <= targetTime
	idx := sort.Search(len(td.Configs), func(i int) bool {
		return td.Configs[i].Time > targetTime
	})

	if idx == 0 {
		return nil
	}

	return &td.Configs[idx-1]
}

// GetLatestRecoveryStateAtIndex returns the latest recovery state at or before the given event index
func (td *TraceData) GetLatestRecoveryStateAtIndex(eventIndex int) *RecoveryState {
	// Binary search to find the latest recovery state with EventIndex <= eventIndex
	idx := sort.Search(len(td.RecoveryStates), func(i int) bool {
		return td.RecoveryStates[i].EventIndex > eventIndex
	})

	if idx == 0 {
		return nil
	}

	return &td.RecoveryStates[idx-1]
}

// GetLatestEpochVersionAtIndex returns the latest epoch version info at or before the given event index
func (td *TraceData) GetLatestEpochVersionAtIndex(eventIndex int) *EpochVersionInfo {
	// Binary search to find the latest epoch version with EventIndex <= eventIndex
	idx := sort.Search(len(td.EpochVersions), func(i int) bool {
		return td.EpochVersions[i].EventIndex > eventIndex
	})

	if idx == 0 {
		return nil
	}

	return &td.EpochVersions[idx-1]
}

// FindPreviousRecovery finds the latest MasterRecoveryState before the given event index
func (td *TraceData) FindPreviousRecovery(eventIndex int) *RecoveryState {
	// Binary search to find where to start looking
	idx := sort.Search(len(td.RecoveryStates), func(i int) bool {
		return td.RecoveryStates[i].EventIndex >= eventIndex
	})

	// Walk backwards from idx-1 to find any recovery state
	if idx > 0 {
		return &td.RecoveryStates[idx-1]
	}

	return nil
}

// FindNextRecovery finds the earliest MasterRecoveryState after the given event index
func (td *TraceData) FindNextRecovery(eventIndex int) *RecoveryState {
	// Binary search to find where to start looking
	// Use > to skip the current event index
	idx := sort.Search(len(td.RecoveryStates), func(i int) bool {
		return td.RecoveryStates[i].EventIndex > eventIndex
	})

	// Return first recovery state after current index
	if idx < len(td.RecoveryStates) {
		return &td.RecoveryStates[idx]
	}

	return nil
}

// FindPreviousRecoveryWithStatusCode finds the latest recovery state before the given event index with the specified status code
func (td *TraceData) FindPreviousRecoveryWithStatusCode(eventIndex int, statusCode string) *RecoveryState {
	// Binary search to find where to start looking
	idx := sort.Search(len(td.RecoveryStates), func(i int) bool {
		return td.RecoveryStates[i].EventIndex >= eventIndex
	})

	// Walk backwards from idx-1 to find the first match
	// Start from idx-1 to skip any recovery at exactly eventIndex
	for i := idx - 1; i >= 0; i-- {
		if td.RecoveryStates[i].StatusCode == statusCode {
			return &td.RecoveryStates[i]
		}
	}

	return nil
}

// FindNextRecoveryWithStatusCode finds the earliest recovery state after the given event index with the specified status code
func (td *TraceData) FindNextRecoveryWithStatusCode(eventIndex int, statusCode string) *RecoveryState {
	// Binary search to find where to start looking
	// Use > to skip the current event index
	idx := sort.Search(len(td.RecoveryStates), func(i int) bool {
		return td.RecoveryStates[i].EventIndex > eventIndex
	})

	// Walk forwards from idx to find the first match
	for i := idx; i < len(td.RecoveryStates); i++ {
		if td.RecoveryStates[i].StatusCode == statusCode {
			return &td.RecoveryStates[i]
		}
	}

	return nil
}

// GetEventIndexAtTime finds the index of the first event at or closest to targetTime
func (td *TraceData) GetEventIndexAtTime(targetTime float64) int {
	// Binary search to find the first event at targetTime or later
	idx := sort.Search(len(td.Events), func(i int) bool {
		return td.Events[i].TimeValue >= targetTime
	})

	if idx >= len(td.Events) {
		// If no event at or after targetTime, return last event
		return len(td.Events) - 1
	}

	// If we found an event at exactly targetTime, return it
	if td.Events[idx].TimeValue == targetTime {
		return idx
	}

	// Otherwise, check if the previous event is closer
	if idx > 0 {
		prevDiff := targetTime - td.Events[idx-1].TimeValue
		nextDiff := td.Events[idx].TimeValue - targetTime
		if prevDiff < nextDiff {
			return idx - 1
		}
	}

	return idx
}
