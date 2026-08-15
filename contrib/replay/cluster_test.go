package main

import "testing"

// The markers here are the ones fdbserver actually emits. An earlier version of
// this code filtered an all-ffs sentinel, which does not exist anywhere in the
// tree, while letting the real "[not set]" through.
func TestIsSetID(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
		why  string
	}{
		{"a1b2c3d4e5f60718", true, "a real 16-hex-digit shortString"},
		{"a1b2c3d4e5f60718a1b2c3d4e5f60718", true, "a full 32-hex-digit UID"},
		{"[not set]", false, "Traceable<Optional<UID>> when absent"},
		{"unknown", false, "StorageServerSourceTLogID with no source TLog"},
		{"", false, "missing attribute"},
		{"ffffffffffffffff", true, "not a sentinel in fdbserver; must not be filtered"},
	} {
		if got := isSetID(tc.in); got != tc.want {
			t.Errorf("isSetID(%q) = %v, want %v (%s)", tc.in, got, tc.want, tc.why)
		}
	}
}

// A buddy reference that is not a real ID must never reach a role label, even
// before the active-topology filter gets a chance to drop it.
func TestUnsetBuddyMarkersAreNotStored(t *testing.T) {
	const lrID = "3333333333333333"
	const ssID = "1111111111111111"
	m := "[abcd::2:1:1:0]:1"

	events := []TraceEvent{
		{Type: "Role", Machine: m, ID: lrID, Attrs: map[string]string{"Transition": "Begin", "As": "LogRouter"}},
		{Type: "Role", Machine: m, ID: ssID, Attrs: map[string]string{"Transition": "Begin", "As": "StorageServer"}},
		{Type: "LogRouterMetrics", Machine: m, ID: lrID, Attrs: map[string]string{"PrimaryPeekLocation": "[not set]"}},
		{Type: "LogRouterPeekLocation", Machine: m, ID: lrID, Attrs: map[string]string{"LogID": "[not set]"}},
		{Type: "StorageServerSourceTLogID", Machine: m, ID: ssID, Attrs: map[string]string{"SourceTLogID": "unknown"}},
	}

	st := BuildClusterState(events)
	for _, r := range st.Workers[m].Roles {
		if r.BuddyID != "" {
			t.Errorf("role %s [%s] got BuddyID %q, want empty", r.Name, r.ID, r.BuddyID)
		}
	}
}
