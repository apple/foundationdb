package main

import (
	"regexp"
	"strings"
)

// RoleInfo represents a role with its ID
type RoleInfo struct {
	Name  string // e.g., "StorageServer", "Coordinator"
	ID    string // e.g., "f5f3670ef3675364"
	Epoch string // Generation/Epoch for TLog, LogRouter, BackupWorker (empty for others)
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
		case "LogRouterMetrics":
			// LogRouter epoch from metrics (fallback if start event missed)
			if generation, ok := event.Attrs["Generation"]; ok && event.ID != "" {
				if _, exists := epochByID[event.ID]; !exists {
					epochByID[event.ID] = generation
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

	// Second pass: Update roles with epoch info that may have arrived after the Role event
	for _, worker := range state.Workers {
		for i := range worker.Roles {
			if worker.Roles[i].Epoch == "" {
				if epoch, ok := epochByID[worker.Roles[i].ID]; ok {
					worker.Roles[i].Epoch = epoch
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
