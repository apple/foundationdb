package main

import (
	"regexp"
	"strconv"
	"strings"
)

// isSetID reports whether a UID-valued trace detail actually holds an ID.
//
// fdbserver has two ways of saying "no ID here", and neither is an all-ffs
// sentinel:
//
//   - "[not set]" for an Optional<UID> passed straight to detail(), which is what
//     Traceable<Optional<T>> renders when the value is absent. This is the case
//     for LogRouterMetrics.PrimaryPeekLocation and LogRouterPeekLocation.LogID,
//     both of which come from IReplayPeekCursor::getPrimaryPeekLocation() and are
//     absent whenever the cursor has no chosen best server.
//   - "unknown", written explicitly by StorageServerSourceTLogID when the storage
//     server has no source TLog yet.
//
// Note also that a UID reaching a trace via Traceable<UID> is only the first 16
// hex digits, while one written with UID::toString() is the full 32; callers
// truncate to 16 to match the Role event's ID format.
func isSetID(v string) bool {
	return v != "" && v != "[not set]" && v != "unknown"
}

// RoleInfo represents a role with its ID
type RoleInfo struct {
	Name     string   // e.g., "StorageServer", "Coordinator"
	ID       string   // e.g., "f5f3670ef3675364"
	Epoch    string   // Generation/Epoch for TLog, LogRouter, BackupWorker (empty for others)
	Version  int64    // Latest processed version for TLog, StorageServer, LogRouter (0 if unknown)
	BuddyID  string   // Buddy relationship: SS->TLog, LogRouter->TLog it peeks from (empty if unknown)
	BuddyIDs []string // For Remote TLog: list of LogRouter IDs it pulls from (1:N relationship)
}

// Worker represents a process in the cluster
type Worker struct {
	Machine     string     // e.g., "[abcd::2:1:1:0]:1"
	Roles       []RoleInfo // Roles assigned to this worker (including "Worker" role)
	MachineType string     // "main" or "tester"
	DCID        string     // e.g., "0", "1", "2", etc.
}

// ClusterState represents the state of the cluster at a given time
type ClusterState struct {
	Workers map[string]*Worker // Key: Machine address
}

// NewClusterState creates a new empty cluster state
func NewClusterState() *ClusterState {
	return &ClusterState{
		Workers: make(map[string]*Worker),
	}
}

// parseAddress extracts machine type and DC ID from address
// Format 1: [abcd::X:Y:Z:W]:Port where X=type (2=main, 3=tester), Y=DC ID
// Format 2: X.Y.Z.W:Port where X=type (2=main, 3=tester), Y=DC ID
func parseAddress(address string) (machineType string, dcID string) {
	// Default values
	machineType = "unknown"
	dcID = "unknown"

	// Try format 1: [abcd::2:1:1:0]:1
	re1 := regexp.MustCompile(`\[abcd::(\d+):(\d+):`)
	matches := re1.FindStringSubmatch(address)

	if len(matches) >= 3 {
		typeNum := matches[1]
		dcNum := matches[2]

		if typeNum == "2" {
			machineType = "main"
		} else if typeNum == "3" {
			machineType = "tester"
		}

		dcID = dcNum
		return machineType, dcID
	}

	// Try format 2: 2.0.1.3:1
	re2 := regexp.MustCompile(`^(\d+)\.(\d+)\.`)
	matches = re2.FindStringSubmatch(address)

	if len(matches) >= 3 {
		typeNum := matches[1]
		dcNum := matches[2]

		if typeNum == "2" {
			machineType = "main"
		} else if typeNum == "3" {
			machineType = "tester"
		}

		dcID = dcNum
		return machineType, dcID
	}

	return machineType, dcID
}

// BuildClusterState builds the cluster state from events up to a given time
func BuildClusterState(events []TraceEvent) *ClusterState {
	state := NewClusterState()

	// Map to track epoch info by role ID (from metrics events)
	epochByID := make(map[string]string)
	// Map to track version by role ID (from metrics events)
	versionByID := make(map[string]int64)
	// Map to track buddy ID by role ID (SS->TLog, LogRouter->TLog)
	buddyByID := make(map[string]string)
	// Map to track buddy IDs list by role ID (Remote TLog->LogRouters, 1:N)
	buddyIDsByID := make(map[string][]string)

	for _, event := range events {
		// Extract epoch info from start events (preferred - happens at initialization)
		// and metrics events (fallback - happens periodically)
		switch event.Type {
		case "TLogStart":
			// TLog epoch is in the "RecoveryCount" attribute
			if recoveryCount, ok := event.Attrs["RecoveryCount"]; ok && event.ID != "" {
				epochByID[event.ID] = recoveryCount
			}
		case "LogRouterStart":
			// LogRouter epoch is in the "Epoch" attribute
			if epoch, ok := event.Attrs["Epoch"]; ok && event.ID != "" {
				epochByID[event.ID] = epoch
			}
		case "BackupWorkerStart":
			// BackupWorker recruited epoch is in "LogEpoch" attribute
			if logEpoch, ok := event.Attrs["LogEpoch"]; ok && event.ID != "" {
				epochByID[event.ID] = logEpoch
			}
		case "TLogMetrics":
			// TLog epoch from metrics (fallback if start event missed)
			if generation, ok := event.Attrs["Generation"]; ok && event.ID != "" {
				if _, exists := epochByID[event.ID]; !exists {
					epochByID[event.ID] = generation
				}
			}
			// TLog version from metrics
			if version, ok := event.Attrs["Version"]; ok && event.ID != "" {
				if v, err := strconv.ParseInt(version, 10, 64); err == nil {
					versionByID[event.ID] = v
				}
			}
		case "LogRouterMetrics":
			// LogRouter epoch from metrics (fallback if start event missed)
			if generation, ok := event.Attrs["Generation"]; ok && event.ID != "" {
				if _, exists := epochByID[event.ID]; !exists {
					epochByID[event.ID] = generation
				}
			}
			// LogRouter version from metrics
			if version, ok := event.Attrs["Version"]; ok && event.ID != "" {
				if v, err := strconv.ParseInt(version, 10, 64); err == nil {
					versionByID[event.ID] = v
				}
			}
			// LogRouter buddy from metrics (PrimaryPeekLocation)
			if peekLocation, ok := event.Attrs["PrimaryPeekLocation"]; ok && event.ID != "" {
				if isSetID(peekLocation) {
					buddyByID[event.ID] = peekLocation
				}
			}
		case "StorageMetrics":
			// StorageServer version from metrics
			if version, ok := event.Attrs["Version"]; ok && event.ID != "" {
				if v, err := strconv.ParseInt(version, 10, 64); err == nil {
					versionByID[event.ID] = v
				}
			}
		case "StorageServerSourceTLogID":
			// StorageServer buddy - which TLog it peeks from
			if sourceTLogID, ok := event.Attrs["SourceTLogID"]; ok && event.ID != "" {
				if isSetID(sourceTLogID) {
					buddyByID[event.ID] = sourceTLogID
				}
			}
		case "LogRouterPeekLocation":
			// LogRouter buddy - which TLog it peeks from
			if logID, ok := event.Attrs["LogID"]; ok && event.ID != "" {
				if isSetID(logID) {
					buddyByID[event.ID] = logID
				}
			}
		case "TLogPeekRemoteBestOnly":
			// Remote TLog - list of LogRouters it pulls from (1:N relationship)
			if logRouterIds, ok := event.Attrs["LogRouterIds"]; ok && event.ID != "" {
				// Parse comma-separated list of LogRouter IDs
				// Format: "fe567807fe9a2aa3f2ada4c5b58afa9c, 8b4de5474b14b0f05356203f4c4c5034, ..."
				var ids []string
				for _, id := range strings.Split(logRouterIds, ",") {
					id = strings.TrimSpace(id)
					if id != "" {
						// Extract just the first 16 chars (the actual ID, not the full UID)
						if len(id) > 16 {
							id = id[:16]
						}
						ids = append(ids, id)
					}
				}
				if len(ids) > 0 {
					buddyIDsByID[event.ID] = ids
				}
			}
		}

		if event.Type == "Role" && event.Machine != "0.0.0.0:0" {
			transition := event.Attrs["Transition"]
			roleName := event.Attrs["As"]
			roleID := event.ID

			// Skip if no role name
			if roleName == "" {
				continue
			}

			// Get or create worker
			worker, exists := state.Workers[event.Machine]
			if !exists {
				machineType, dcID := parseAddress(event.Machine)
				worker = &Worker{
					Machine:     event.Machine,
					Roles:       []RoleInfo{},
					MachineType: machineType,
					DCID:        dcID,
				}
				state.Workers[event.Machine] = worker
			}

			// Handle role transitions (including "Worker" role)
			if transition == "Begin" {
				// Add role if not already present
				hasRole := false
				for _, r := range worker.Roles {
					if r.Name == roleName && r.ID == roleID {
						hasRole = true
						break
					}
				}
				if !hasRole {
					worker.Roles = append(worker.Roles, RoleInfo{
						Name:  roleName,
						ID:    roleID,
						Epoch: epochByID[roleID], // May be empty if metrics not seen yet
					})
				}
			} else if transition == "End" {
				// Remove role with matching name and ID
				newRoles := []RoleInfo{}
				for _, r := range worker.Roles {
					if !(r.Name == roleName && r.ID == roleID) {
						newRoles = append(newRoles, r)
					}
				}
				worker.Roles = newRoles
			}
			// "Refresh" transitions don't change state, just skip them
		}
	}

	// Second pass: Build set of active role IDs for validating buddy references
	activeRoleIDs := make(map[string]bool)
	for _, worker := range state.Workers {
		for _, role := range worker.Roles {
			if role.ID != "" {
				activeRoleIDs[role.ID] = true
			}
		}
	}

	// Third pass: Update roles with epoch, version, and buddy info that may have arrived after the Role event
	// Only include buddy references if the buddy still exists in the current topology
	for _, worker := range state.Workers {
		for i := range worker.Roles {
			if worker.Roles[i].Epoch == "" {
				if epoch, ok := epochByID[worker.Roles[i].ID]; ok {
					worker.Roles[i].Epoch = epoch
				}
			}
			// Update version from metrics
			if version, ok := versionByID[worker.Roles[i].ID]; ok {
				worker.Roles[i].Version = version
			}
			// Update buddy info (1:1 relationships: SS->TLog, LogRouter->TLog)
			// Only if the buddy still exists in topology
			// Truncate to 16 chars to match Role ID format
			if buddy, ok := buddyByID[worker.Roles[i].ID]; ok {
				buddyShort := buddy
				if len(buddyShort) > 16 {
					buddyShort = buddyShort[:16]
				}
				if activeRoleIDs[buddyShort] {
					worker.Roles[i].BuddyID = buddyShort
				}
			}
			// Update buddy IDs list (1:N relationships: TLog->LogRouters)
			// Only include LogRouters that still exist in topology
			if buddyIDs, ok := buddyIDsByID[worker.Roles[i].ID]; ok {
				var activeIDs []string
				for _, id := range buddyIDs {
					if activeRoleIDs[id] {
						activeIDs = append(activeIDs, id)
					}
				}
				if len(activeIDs) > 0 {
					worker.Roles[i].BuddyIDs = activeIDs
				}
			}
		}
	}

	return state
}

// GetWorkersByDC returns workers grouped by DC ID (main machines only)
func (cs *ClusterState) GetWorkersByDC() map[string][]*Worker {
	dcMap := make(map[string][]*Worker)

	for _, w := range cs.Workers {
		if w.MachineType == "main" {
			dcMap[w.DCID] = append(dcMap[w.DCID], w)
		}
	}

	// Sort workers within each DC by machine address for consistent ordering
	for _, workers := range dcMap {
		for i := 0; i < len(workers); i++ {
			for j := i + 1; j < len(workers); j++ {
				if workers[i].Machine > workers[j].Machine {
					workers[i], workers[j] = workers[j], workers[i]
				}
			}
		}
	}

	return dcMap
}

// GetTesters returns all tester workers
func (cs *ClusterState) GetTesters() []*Worker {
	testers := []*Worker{}

	for _, w := range cs.Workers {
		if w.MachineType == "tester" {
			testers = append(testers, w)
		}
	}

	// Sort testers by machine address for consistent ordering
	for i := 0; i < len(testers); i++ {
		for j := i + 1; j < len(testers); j++ {
			if testers[i].Machine > testers[j].Machine {
				testers[i], testers[j] = testers[j], testers[i]
			}
		}
	}

	return testers
}

// HasRoles returns true if the worker has any roles assigned
func (w *Worker) HasRoles() bool {
	return len(w.Roles) > 0
}

// HasNonWorkerRoles returns true if the worker has any roles other than "Worker"
func (w *Worker) HasNonWorkerRoles() bool {
	for _, role := range w.Roles {
		if role.Name != "Worker" {
			return true
		}
	}
	return false
}

// RolesString returns a comma-separated string of roles
func (w *Worker) RolesString() string {
	if len(w.Roles) == 0 {
		return ""
	}
	roleNames := make([]string, len(w.Roles))
	for i, r := range w.Roles {
		roleNames[i] = r.Name
	}
	return strings.Join(roleNames, ", ")
}
