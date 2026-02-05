package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// model holds the UI state for the Bubbletea application
type model struct {
	traceData         *TraceData
	currentTime       float64
	currentEventIndex int // Index of current event in traceData.Events
	clusterState      *ClusterState
	width             int
	height            int
	timeInputMode     bool
	timeInput         textinput.Model
	configViewMode    bool
	configScrollOffset int // Vertical scroll offset for config popup
	helpViewMode      bool // Help popup mode
	healthViewMode    bool // Health popup mode
	healthScrollOffset int // Vertical scroll offset for health popup
	searchMode        bool // Search input mode
	searchDirection   string // "forward" or "backward"
	searchInput       textinput.Model
	searchPattern     string // Current search pattern
	searchActive      bool // Whether search highlighting is active
	// Filter state
	filterViewMode          bool     // Filter popup mode
	filterShowAll           bool     // Whether "All" is checked (default true)
	filterCurrentCategory   int      // 0=Raw, 1=Machine, 2=Time, 3=Message
	// Category 1: Raw filters (OR logic)
	filterRawList           []string // Raw filter patterns
	filterRawInput          textinput.Model
	filterRawSelectedIndex  int
	filterRawInputActive    bool
	filterRawColumn         int           // Current column in raw filter list (for ctrl+f/ctrl+b navigation)
	filterRawDisabled       map[int]bool  // Track disabled raw filters (map[index]disabled)
	filterRawCompiledRegex  []*regexp.Regexp // Pre-compiled regex patterns for raw filters (parallel to filterRawList)
	filterTypeSearchMode    bool          // Type search popup mode
	filterTypeSearchInput   textinput.Model
	filterTypeSearchList    []string      // All unique Type values from trace
	filterTypeSearchSelected int          // Currently selected Type in list
	// Category 2: Machine filters (OR logic)
	filterMachineList       []string // Selected machine addresses
	filterMachineSet        map[string]bool // Set for O(1) lookup of selected machines
	filterMachineSelectMode bool     // In machine selection popup
	filterMachineInput      textinput.Model // For fuzzy search in machine popup
	filterMachineDCs        map[string]bool // Selected DCs (map[dcID]selected)
	filterMachineScroll     int // Scroll offset in machine popup (deprecated, using columns now)
	filterMachineSelected   int // Currently highlighted item in machine popup
	filterMachineColumn     int // Current column in machine popup (for ctrl+f/ctrl+b navigation)
	// Category 3: Time range filter
	filterTimeEnabled       bool    // Whether time filter is active
	filterTimeStart         float64 // Start time
	filterTimeEnd           float64 // End time
	filterTimeInputMode     bool    // Configuring time range
	filterTimeEditingStart  bool    // true=editing start, false=editing end
	filterTimeInput         textinput.Model
	// Category 4: Message filter (NetworkMessageSent events only)
	filterMessageEnabled    bool    // Whether message filter is active
	// Performance caches
	machineDCCache          map[string]string // Cache DC extraction results (map[machineAddr]dcID)
}

// newModel creates a new model with the given trace data
func newModel(traceData *TraceData) model {
	ti := textinput.New()
	ti.Placeholder = "Enter time in seconds (e.g., 123.456)"
	ti.CharLimit = 20
	ti.Width = 40

	si := textinput.New()
	si.Placeholder = "Enter search pattern (use * for wildcard)"
	si.CharLimit = 100
	si.Width = 60

	// Raw filter input
	rawInput := textinput.New()
	rawInput.Placeholder = "Enter raw filter pattern (e.g., Type=WorkerHealthMonitor or Role*TL)"
	rawInput.CharLimit = 100
	rawInput.Width = 70

	// Machine filter fuzzy search input
	machineInput := textinput.New()
	machineInput.Placeholder = "Type to filter machines..."
	machineInput.CharLimit = 50
	machineInput.Width = 60

	// Time filter input
	timeFilterInput := textinput.New()
	timeFilterInput.Placeholder = "Enter time (e.g., 4.5)"
	timeFilterInput.CharLimit = 20
	timeFilterInput.Width = 30

	// Type search input
	typeSearchInput := textinput.New()
	typeSearchInput.Placeholder = "Type to search event types..."
	typeSearchInput.CharLimit = 50
	typeSearchInput.Width = 60

	// Extract all unique Type values from trace data
	typeSet := make(map[string]bool)
	for _, event := range traceData.Events {
		if event.Type != "" {
			typeSet[event.Type] = true
		}
	}
	var allTypes []string
	for t := range typeSet {
		allTypes = append(allTypes, t)
	}
	sort.Strings(allTypes)

	return model{
		traceData:          traceData,
		currentTime:        0.0,
		currentEventIndex:  0,
		clusterState:       NewClusterState(),
		timeInputMode:      false,
		timeInput:          ti,
		configViewMode:     false,
		configScrollOffset: 0,
		helpViewMode:       false,
		healthViewMode:     false,
		healthScrollOffset: 0,
		searchMode:         false,
		searchDirection:    "",
		searchInput:        si,
		searchPattern:      "",
		searchActive:       false,
		// Filter initialization
		filterViewMode:         false,
		filterShowAll:          true, // Default: show all events
		filterCurrentCategory:  0,    // Start in Raw category
		filterRawList:          []string{},
		filterRawInput:         rawInput,
		filterRawSelectedIndex: -1,
		filterRawInputActive:   false,
		filterRawColumn:        0,
		filterRawDisabled:      make(map[int]bool),
		filterRawCompiledRegex: []*regexp.Regexp{},
		filterTypeSearchMode:    false,
		filterTypeSearchInput:   typeSearchInput,
		filterTypeSearchList:    allTypes,
		filterTypeSearchSelected: 0,
		filterMachineList:      []string{},
		filterMachineSet:       make(map[string]bool),
		filterMachineSelectMode: false,
		filterMachineInput:     machineInput,
		filterMachineDCs:       make(map[string]bool),
		filterMachineScroll:    0,
		filterMachineSelected:  0,
		filterMachineColumn:    0,
		filterTimeEnabled:      false,
		filterTimeStart:        traceData.MinTime,
		filterTimeEnd:          traceData.MaxTime,
		filterTimeInputMode:    false,
		filterTimeEditingStart: true,
		filterTimeInput:        timeFilterInput,
		filterMessageEnabled:   false,
		machineDCCache:         make(map[string]string),
	}
}

// Init initializes the model (required by Bubbletea)
func (m model) Init() tea.Cmd {
	return nil
}

// Update handles messages and updates the model (required by Bubbletea)
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	// Handle filter view mode
	if m.filterViewMode {
		// If in machine selection popup
		if m.filterMachineSelectMode {
			return m.handleMachineSelectionPopup(msg)
		}

		// If in time input mode within filter
		if m.filterTimeInputMode {
			return m.handleFilterTimeInput(msg)
		}

		// If in Type search popup
		if m.filterTypeSearchMode {
			return m.handleTypeSearchPopup(msg)
		}

		switch msg := msg.(type) {
		case tea.KeyMsg:
			// Handle raw filter input mode
			if m.filterRawInputActive {
				switch msg.String() {
				case "enter":
					if filterText := m.filterRawInput.Value(); filterText != "" {
						// Check if we're editing an existing filter or adding new
						if m.filterRawSelectedIndex >= 0 && m.filterRawSelectedIndex < len(m.filterRawList) {
							// Editing existing - check if original value is still in input
							// If user entered edit mode via enter, replace the filter
							m.filterRawList[m.filterRawSelectedIndex] = filterText
						} else {
							// Adding new
							m.filterRawList = append(m.filterRawList, filterText)
							m.filterRawSelectedIndex = len(m.filterRawList) - 1
						}
						// Recompile regex patterns for performance
						m.recompileRawFilterRegexes()
						m.filterRawInput.Reset()
						m.filterRawInput.Blur()
						m.filterRawInputActive = false
					}
					return m, nil
				case "esc", "ctrl+c":
					m.filterRawInput.Reset()
					m.filterRawInput.Blur()
					m.filterRawInputActive = false
					return m, nil
				default:
					m.filterRawInput, cmd = m.filterRawInput.Update(msg)
					return m, cmd
				}
			}

			// Handle keys when not in input mode
			switch msg.String() {
			case "q", "f", "esc", "ctrl+c":
				m.filterViewMode = false
				m.filterRawInput.Reset()
				// Ensure current event is visible after filter changes
				m.ensureCurrentEventVisible()
				return m, nil

			case "space", " ":
				// Toggle "All" checkbox
				m.filterShowAll = !m.filterShowAll
				// Ensure current event is visible after toggling
				m.ensureCurrentEventVisible()
				return m, nil

			case "ctrl+n":
				// Move to next category OR navigate down in raw filter list
				if !m.filterShowAll {
					// If in Raw category with filters, try to navigate within the list first
					if m.filterCurrentCategory == 0 && len(m.filterRawList) > 0 {
						maxRows := 5
						itemsPerColumn := maxRows

						// Calculate the start and end index for current column
						startIdx := m.filterRawColumn * itemsPerColumn
						endIdx := startIdx + itemsPerColumn
						if endIdx > len(m.filterRawList) {
							endIdx = len(m.filterRawList)
						}

						// Check if we can move down within this column
						if m.filterRawSelectedIndex < endIdx - 1 {
							// Move down within this column
							m.filterRawSelectedIndex++
							return m, nil
						} else {
							// At the end of column, switch to next category
							m.filterCurrentCategory = (m.filterCurrentCategory + 1) % 4
							return m, nil
						}
					} else {
						// Not in Raw category or no filters, just switch category
						m.filterCurrentCategory = (m.filterCurrentCategory + 1) % 4
					}
				}
				return m, nil

			case "ctrl+p":
				// Move to previous category OR navigate up in raw filter list
				if !m.filterShowAll {
					// If in Raw category with filters, try to navigate within the list first
					if m.filterCurrentCategory == 0 && len(m.filterRawList) > 0 {
						maxRows := 5
						itemsPerColumn := maxRows

						// Calculate the start and end index for current column
						startIdx := m.filterRawColumn * itemsPerColumn

						// Check if we can move up within this column
						if m.filterRawSelectedIndex > startIdx {
							// Move up within this column
							m.filterRawSelectedIndex--
							return m, nil
						} else {
							// At the start of column, switch to previous category
							m.filterCurrentCategory = (m.filterCurrentCategory - 1 + 4) % 4
							return m, nil
						}
					} else {
						// Not in Raw category or no filters, just switch category
						m.filterCurrentCategory = (m.filterCurrentCategory - 1 + 4) % 4
					}
				}
				return m, nil

			case "1":
				// Jump to Raw category
				if !m.filterShowAll {
					m.filterCurrentCategory = 0
				}
				return m, nil

			case "2":
				// Jump to Machine category
				if !m.filterShowAll {
					m.filterCurrentCategory = 1
				}
				return m, nil

			case "3":
				// Jump to Time category
				if !m.filterShowAll {
					m.filterCurrentCategory = 2
				}
				return m, nil

			case "4":
				// Jump to Message category
				if !m.filterShowAll {
					m.filterCurrentCategory = 3
				}
				return m, nil

			case "a":
				// Add new filter (only in Raw category)
				if !m.filterShowAll && m.filterCurrentCategory == 0 {
					m.filterRawInputActive = true
					m.filterRawSelectedIndex = -1 // Mark as adding new (not editing)
					m.filterRawInput.Focus()
					return m, textinput.Blink
				}
				return m, nil

			case "r":
				// Remove selected filter (only in Raw category)
				if !m.filterShowAll && m.filterCurrentCategory == 0 {
					if m.filterRawSelectedIndex >= 0 && m.filterRawSelectedIndex < len(m.filterRawList) {
						// Remove filter and update disabled map
						removedIdx := m.filterRawSelectedIndex
						m.filterRawList = append(m.filterRawList[:removedIdx], m.filterRawList[removedIdx+1:]...)

						// Rebuild disabled map, shifting indices
						newDisabled := make(map[int]bool)
						for i, disabled := range m.filterRawDisabled {
							if i < removedIdx {
								newDisabled[i] = disabled
							} else if i > removedIdx {
								newDisabled[i-1] = disabled
							}
							// Skip i == removedIdx (the removed filter)
						}
						m.filterRawDisabled = newDisabled

						// Recompile regex patterns for performance
						m.recompileRawFilterRegexes()

						// Update selection
						if m.filterRawSelectedIndex >= len(m.filterRawList) {
							m.filterRawSelectedIndex = len(m.filterRawList) - 1
						}
						if m.filterRawSelectedIndex < 0 {
							m.filterRawSelectedIndex = 0
						}

						// Update column index
						if len(m.filterRawList) > 0 {
							maxRows := 5
							m.filterRawColumn = m.filterRawSelectedIndex / maxRows
						} else {
							m.filterRawColumn = 0
						}
					}
				}
				return m, nil

			case "e":
				// Edit selected filter (only in Raw category)
				if !m.filterShowAll && m.filterCurrentCategory == 0 {
					if m.filterRawSelectedIndex >= 0 && m.filterRawSelectedIndex < len(m.filterRawList) {
						m.filterRawInputActive = true
						m.filterRawInput.SetValue(m.filterRawList[m.filterRawSelectedIndex])
						m.filterRawInput.Focus()
						// We'll replace this filter when user presses enter again
						return m, textinput.Blink
					}
				}
				return m, nil

			case "d":
				// Toggle disabled state for selected raw filter OR toggle time filter OR toggle message filter
				if !m.filterShowAll {
					if m.filterCurrentCategory == 0 {
						// Raw category: toggle disabled state for selected filter
						if m.filterRawSelectedIndex >= 0 && m.filterRawSelectedIndex < len(m.filterRawList) {
							m.filterRawDisabled[m.filterRawSelectedIndex] = !m.filterRawDisabled[m.filterRawSelectedIndex]
							// Ensure current event is visible after toggling
							m.ensureCurrentEventVisible()
						}
					} else if m.filterCurrentCategory == 2 {
						// Time category: toggle time filter enabled/disabled
						m.filterTimeEnabled = !m.filterTimeEnabled
						// Ensure current event is visible after toggling
						m.ensureCurrentEventVisible()
					} else if m.filterCurrentCategory == 3 {
						// Message category: toggle message filter enabled/disabled
						m.filterMessageEnabled = !m.filterMessageEnabled
						// Ensure current event is visible after toggling
						m.ensureCurrentEventVisible()
					}
				}
				return m, nil

			case "t":
				// Open Type search popup (only in Raw category)
				if m.filterCurrentCategory == 0 {
					m.filterTypeSearchMode = true
					m.filterTypeSearchInput.Focus()
					m.filterTypeSearchSelected = 0
					return m, textinput.Blink
				}
				return m, nil

			case "c":
				// Toggle common trace event filters (only in Raw category)
				if m.filterCurrentCategory == 0 {
					// Define common trace event types
					commonTypes := []string{
						"Type=MasterRecoveryState",
						"Type=ProxyReject",
						"Type=CCWDB",
						"Type=TPLSOnErrorLogSystemFailed",
						"Type=TPLSOnErrorBackupFailed",
						"Type=ForcedRecoveryStart",
						"Type=MasterRegistrationKill",
						"Type=ClusterControllerHealthMonitor",
						"Type=DegradedServerDetectedAndTriggerRecovery",
						"Type=RecentRecoveryCountHigh",
						"Type=DegradedServerDetectedAndSuggestRecovery",
						"Type=DegradedServerDetectedAndTriggerFailover",
						"Type=AddFailureInjectionWorkload",
						"Type=TrackTLogRecovery",
						"Type=MutationTracking",
						"Type=Assassination",
						"Severity=40",
					}

					// Check if any common filters exist
					hasCommonFilters := false
					for _, filter := range m.filterRawList {
						for _, common := range commonTypes {
							if filter == common {
								hasCommonFilters = true
								break
							}
						}
						if hasCommonFilters {
							break
						}
					}

					if hasCommonFilters {
						// Remove all common filters
						var newList []string
						newDisabled := make(map[int]bool)
						newIdx := 0
						for i, filter := range m.filterRawList {
							isCommon := false
							for _, common := range commonTypes {
								if filter == common {
									isCommon = true
									break
								}
							}
							if !isCommon {
								newList = append(newList, filter)
								// Preserve disabled state for non-common filters
								if m.filterRawDisabled[i] {
									newDisabled[newIdx] = true
								}
								newIdx++
							}
						}
						m.filterRawList = newList
						m.filterRawDisabled = newDisabled
						m.filterRawSelectedIndex = 0
						m.filterRawColumn = 0 // Reset to first column
						if m.filterRawSelectedIndex >= len(m.filterRawList) {
							m.filterRawSelectedIndex = len(m.filterRawList) - 1
						}
					} else {
						// Add all common filters (enabled by default)
						startIdx := len(m.filterRawList)
						m.filterRawList = append(m.filterRawList, commonTypes...)
						m.filterRawSelectedIndex = startIdx
						// Update column index to show the new filters
						maxRows := 5
						m.filterRawColumn = m.filterRawSelectedIndex / maxRows
					}

					// Recompile regex patterns for performance
					m.recompileRawFilterRegexes()

					// Update visibility after filter change
					m.ensureCurrentEventVisible()
				}
				return m, nil

			case "enter":
				// Open configuration for Machine or Time category only
				if !m.filterShowAll {
					if m.filterCurrentCategory == 1 {
						// Enter machine selection mode
						m.filterMachineSelectMode = true
						m.filterMachineInput.Focus()
						return m, textinput.Blink
					} else if m.filterCurrentCategory == 2 {
						// Enter time configuration mode
						m.filterTimeInputMode = true
						m.filterTimeEditingStart = true
						m.filterTimeInput.SetValue(fmt.Sprintf("%.6f", m.filterTimeStart))
						m.filterTimeInput.Focus()
						return m, textinput.Blink
					}
				}
				return m, nil

			case "ctrl+f":
				// Move to next column in raw filter list (only in Raw category)
				if !m.filterShowAll && m.filterCurrentCategory == 0 && len(m.filterRawList) > 0 {
					// Pack into columns to determine count
					maxRows := 5
					numColumns := (len(m.filterRawList) + maxRows - 1) / maxRows
					if numColumns > 0 {
						m.filterRawColumn++
						if m.filterRawColumn >= numColumns {
							m.filterRawColumn = 0 // Wrap to first column
						}
						// Set selection to first item in new column
						m.filterRawSelectedIndex = m.filterRawColumn * maxRows
						// Clamp to valid range
						if m.filterRawSelectedIndex >= len(m.filterRawList) {
							m.filterRawSelectedIndex = len(m.filterRawList) - 1
						}
					}
				}
				return m, nil

			case "ctrl+b":
				// Move to previous column in raw filter list (only in Raw category)
				if !m.filterShowAll && m.filterCurrentCategory == 0 && len(m.filterRawList) > 0 {
					// Pack into columns to determine count
					maxRows := 5
					numColumns := (len(m.filterRawList) + maxRows - 1) / maxRows
					if numColumns > 0 {
						m.filterRawColumn--
						if m.filterRawColumn < 0 {
							m.filterRawColumn = numColumns - 1 // Wrap to last column
						}
						// Set selection to first item in new column
						m.filterRawSelectedIndex = m.filterRawColumn * maxRows
						// Clamp to valid range
						if m.filterRawSelectedIndex >= len(m.filterRawList) {
							m.filterRawSelectedIndex = len(m.filterRawList) - 1
						}
					}
				}
				return m, nil
			}
		}
		return m, nil
	}

	// Handle help view mode
	if m.helpViewMode {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "q", "h", "esc", "ctrl+c":
				// Exit help view mode
				m.helpViewMode = false
				return m, nil
			}
		}
		return m, nil
	}

	// Handle health view mode
	if m.healthViewMode {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "q", "x", "esc", "ctrl+c":
				// Exit health view mode
				m.healthViewMode = false
				m.healthScrollOffset = 0 // Reset scroll when exiting
				return m, nil
			case "ctrl+p":
				// Scroll up in health view
				if m.healthScrollOffset > 0 {
					m.healthScrollOffset--
				}
				return m, nil
			case "ctrl+n":
				// Scroll down in health view
				m.healthScrollOffset++
				// Will be clamped in renderHealthPopup
				return m, nil
			}
		}
		return m, nil
	}

	// Handle config view mode
	if m.configViewMode {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "q", "c", "esc", "ctrl+c":
				// Exit config view mode
				m.configViewMode = false
				m.configScrollOffset = 0 // Reset scroll when exiting
				return m, nil
			case "ctrl+p":
				// Scroll up in config view
				if m.configScrollOffset > 0 {
					m.configScrollOffset--
				}
				return m, nil
			case "ctrl+n":
				// Scroll down in config view
				m.configScrollOffset++
				// Will be clamped in renderConfigPopup
				return m, nil
			}
		}
		return m, nil
	}

	// Handle time input mode separately
	if m.timeInputMode {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "enter":
				// Try to parse the time and jump to it
				if timeStr := m.timeInput.Value(); timeStr != "" {
					if targetTime, err := strconv.ParseFloat(timeStr, 64); err == nil {
						// Only jump if within valid range
						if targetTime >= m.traceData.MinTime && targetTime <= m.traceData.MaxTime {
							targetIdx := m.traceData.GetEventIndexAtTime(targetTime)
							// Find nearest visible event from target (search forward first, then backward)
							found := false
							// Try forward
							for i := targetIdx; i < len(m.traceData.Events); i++ {
								if eventMatchesFilters(&m.traceData.Events[i], &m) {
									m.currentEventIndex = i
									m.currentTime = m.traceData.Events[i].TimeValue
									m.updateClusterState()
									found = true
									break
								}
							}
							// If not found forward, try backward
							if !found {
								for i := targetIdx - 1; i >= 0; i-- {
									if eventMatchesFilters(&m.traceData.Events[i], &m) {
										m.currentEventIndex = i
										m.currentTime = m.traceData.Events[i].TimeValue
										m.updateClusterState()
										break
									}
								}
							}
							// Exit time input mode
							m.timeInputMode = false
							m.timeInput.Reset()
						}
						// If invalid, stay in input mode so user can see the error and correct it
						return m, nil
					}
				}
				// If empty or parse error, stay in input mode
				return m, nil

			case "esc", "ctrl+c", "q", "t":
				// Cancel time input
				m.timeInputMode = false
				m.timeInput.Reset()
				return m, nil
			}
		}

		// Update the text input
		m.timeInput, cmd = m.timeInput.Update(msg)
		return m, cmd
	}

	// Handle search mode separately
	if m.searchMode {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "enter":
				// Perform search with the entered pattern
				if searchText := m.searchInput.Value(); searchText != "" {
					m.searchPattern = searchText
					m.searchActive = true

					// Search for match
					var matchIndex int
					if m.searchDirection == "forward" {
						matchIndex = m.searchForward(m.currentEventIndex + 1, m.searchPattern)
					} else {
						matchIndex = m.searchBackward(m.currentEventIndex - 1, m.searchPattern)
					}

					if matchIndex >= 0 {
						m.currentEventIndex = matchIndex
						m.currentTime = m.traceData.Events[m.currentEventIndex].TimeValue
						m.updateClusterState()
					}
				}
				// Exit search input mode but keep search active
				m.searchMode = false
				m.searchInput.Blur()
				return m, nil

			case "esc", "ctrl+c":
				// Cancel search input
				m.searchMode = false
				m.searchInput.Reset()
				m.searchInput.Blur()
				return m, nil
			}
		}

		// Update the search input
		m.searchInput, cmd = m.searchInput.Update(msg)
		return m, cmd
	}

	// Normal mode (not in time input)
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "Q":
			return m, tea.Quit

		case "t":
			// Enter time input mode
			m.timeInputMode = true
			m.timeInput.Focus()
			return m, textinput.Blink

		case "c":
			// Enter config view mode
			m.configViewMode = true
			return m, nil

		case "h":
			// Enter help view mode
			m.helpViewMode = true
			return m, nil

		case "x":
			// Enter health view mode
			m.healthViewMode = true
			return m, nil

		case "f":
			// Enter filter view mode
			m.filterViewMode = true
			return m, nil

		case "E", "shift+e":
			// Jump backward to previous MasterRecoveryState (any)
			recovery := m.traceData.FindPreviousRecovery(m.currentEventIndex)
			// If filterShowAll is false and time filter is enabled, keep searching until we find one in range
			for recovery != nil {
				if m.filterShowAll || !m.filterTimeEnabled || (recovery.Time >= m.filterTimeStart && recovery.Time <= m.filterTimeEnd) {
					m.currentEventIndex = recovery.EventIndex
					m.currentTime = recovery.Time
					m.updateClusterState()
					break
				}
				// Try previous recovery
				recovery = m.traceData.FindPreviousRecovery(recovery.EventIndex - 1)
			}

		case "e":
			// Jump forward to next MasterRecoveryState (any)
			recovery := m.traceData.FindNextRecovery(m.currentEventIndex)
			// If filterShowAll is false and time filter is enabled, keep searching until we find one in range
			for recovery != nil {
				if m.filterShowAll || !m.filterTimeEnabled || (recovery.Time >= m.filterTimeStart && recovery.Time <= m.filterTimeEnd) {
					m.currentEventIndex = recovery.EventIndex
					m.currentTime = recovery.Time
					m.updateClusterState()
					break
				}
				// Try next recovery
				recovery = m.traceData.FindNextRecovery(recovery.EventIndex + 1)
			}

		case "ctrl+n":
			// Move forward to next visible (non-filtered) event
			if m.currentEventIndex < len(m.traceData.Events)-1 {
				// Find next event that matches filters
				for i := m.currentEventIndex + 1; i < len(m.traceData.Events); i++ {
					if eventMatchesFilters(&m.traceData.Events[i], &m) {
						m.currentEventIndex = i
						m.currentTime = m.traceData.Events[m.currentEventIndex].TimeValue
						m.updateClusterState()
						break
					}
				}
			}

		case "ctrl+p":
			// Move backward to previous visible (non-filtered) event
			if m.currentEventIndex > 0 {
				// Find previous event that matches filters
				for i := m.currentEventIndex - 1; i >= 0; i-- {
					if eventMatchesFilters(&m.traceData.Events[i], &m) {
						m.currentEventIndex = i
						m.currentTime = m.traceData.Events[m.currentEventIndex].TimeValue
						m.updateClusterState()
						break
					}
				}
			}

		case "ctrl+v":
			// Page forward (1 second) to next visible event
			newTime := m.currentTime + 1.0
			if newTime <= m.traceData.MaxTime {
				targetIdx := m.traceData.GetEventIndexAtTime(newTime)
				// Find next visible event from target
				for i := targetIdx; i < len(m.traceData.Events); i++ {
					if eventMatchesFilters(&m.traceData.Events[i], &m) {
						m.currentEventIndex = i
						m.currentTime = m.traceData.Events[m.currentEventIndex].TimeValue
						m.updateClusterState()
						break
					}
				}
			}

		case "alt+v":
			// Page backward (1 second) to previous visible event
			newTime := m.currentTime - 1.0
			if newTime >= m.traceData.MinTime {
				targetIdx := m.traceData.GetEventIndexAtTime(newTime)
				// Find previous visible event from target
				for i := targetIdx; i >= 0; i-- {
					if eventMatchesFilters(&m.traceData.Events[i], &m) {
						m.currentEventIndex = i
						m.currentTime = m.traceData.Events[m.currentEventIndex].TimeValue
						m.updateClusterState()
						break
					}
				}
			}

		case "g":
			// Jump to start - first visible event
			for i := 0; i < len(m.traceData.Events); i++ {
				if eventMatchesFilters(&m.traceData.Events[i], &m) {
					m.currentEventIndex = i
					m.currentTime = m.traceData.Events[i].TimeValue
					m.updateClusterState()
					break
				}
			}

		case "G", "shift+g":
			// Jump to end - last visible event
			for i := len(m.traceData.Events) - 1; i >= 0; i-- {
				if eventMatchesFilters(&m.traceData.Events[i], &m) {
					m.currentEventIndex = i
					m.currentTime = m.traceData.Events[i].TimeValue
					m.updateClusterState()
					break
				}
			}

		case "R", "shift+r":
			// Jump backward to latest MasterRecoveryState with StatusCode="0"
			recovery := m.traceData.FindPreviousRecoveryWithStatusCode(m.currentEventIndex, "0")
			// If filterShowAll is false and time filter is enabled, keep searching until we find one in range
			for recovery != nil {
				if m.filterShowAll || !m.filterTimeEnabled || (recovery.Time >= m.filterTimeStart && recovery.Time <= m.filterTimeEnd) {
					m.currentEventIndex = recovery.EventIndex
					m.currentTime = recovery.Time
					m.updateClusterState()
					break
				}
				// Try previous recovery with StatusCode=0
				recovery = m.traceData.FindPreviousRecoveryWithStatusCode(recovery.EventIndex-1, "0")
			}

		case "r":
			// Jump forward to earliest MasterRecoveryState with StatusCode="0"
			recovery := m.traceData.FindNextRecoveryWithStatusCode(m.currentEventIndex, "0")
			// If filterShowAll is false and time filter is enabled, keep searching until we find one in range
			for recovery != nil {
				if m.filterShowAll || !m.filterTimeEnabled || (recovery.Time >= m.filterTimeStart && recovery.Time <= m.filterTimeEnd) {
					m.currentEventIndex = recovery.EventIndex
					m.currentTime = recovery.Time
					m.updateClusterState()
					break
				}
				// Try next recovery with StatusCode=0
				recovery = m.traceData.FindNextRecoveryWithStatusCode(recovery.EventIndex+1, "0")
			}

		case "3":
			// Jump forward to next Severity=30 event
			for i := m.currentEventIndex + 1; i < len(m.traceData.Events); i++ {
				event := &m.traceData.Events[i]
				if event.Severity == "30" && eventMatchesFilters(event, &m) {
					m.currentEventIndex = i
					m.currentTime = event.TimeValue
					m.updateClusterState()
					break
				}
			}

		case "#", "shift+3":
			// Jump backward to previous Severity=30 event
			for i := m.currentEventIndex - 1; i >= 0; i-- {
				event := &m.traceData.Events[i]
				if event.Severity == "30" && eventMatchesFilters(event, &m) {
					m.currentEventIndex = i
					m.currentTime = event.TimeValue
					m.updateClusterState()
					break
				}
			}

		case "4":
			// Jump forward to next Severity=40 event
			for i := m.currentEventIndex + 1; i < len(m.traceData.Events); i++ {
				event := &m.traceData.Events[i]
				if event.Severity == "40" && eventMatchesFilters(event, &m) {
					m.currentEventIndex = i
					m.currentTime = event.TimeValue
					m.updateClusterState()
					break
				}
			}

		case "$", "shift+4":
			// Jump backward to previous Severity=40 event
			for i := m.currentEventIndex - 1; i >= 0; i-- {
				event := &m.traceData.Events[i]
				if event.Severity == "40" && eventMatchesFilters(event, &m) {
					m.currentEventIndex = i
					m.currentTime = event.TimeValue
					m.updateClusterState()
					break
				}
			}

		case "/":
			// Always enter forward search mode (start new search)
			m.searchMode = true
			m.searchDirection = "forward"
			m.searchInput.Focus()
			return m, textinput.Blink

		case "?":
			// Always enter backward search mode (start new search)
			m.searchMode = true
			m.searchDirection = "backward"
			m.searchInput.Focus()
			return m, textinput.Blink

		case "n":
			// Go to next match in the original search direction
			if m.searchActive && m.searchPattern != "" {
				var matchIndex int
				if m.searchDirection == "forward" {
					matchIndex = m.searchForward(m.currentEventIndex + 1, m.searchPattern)
				} else {
					matchIndex = m.searchBackward(m.currentEventIndex - 1, m.searchPattern)
				}
				if matchIndex >= 0 {
					m.currentEventIndex = matchIndex
					m.currentTime = m.traceData.Events[m.currentEventIndex].TimeValue
					m.updateClusterState()
				}
			}

		case "N", "shift+n":
			// Go to previous match (opposite of original search direction)
			if m.searchActive && m.searchPattern != "" {
				var matchIndex int
				if m.searchDirection == "forward" {
					// Original was forward, so N goes backward
					matchIndex = m.searchBackward(m.currentEventIndex - 1, m.searchPattern)
				} else {
					// Original was backward, so N goes forward
					matchIndex = m.searchForward(m.currentEventIndex + 1, m.searchPattern)
				}
				if matchIndex >= 0 {
					m.currentEventIndex = matchIndex
					m.currentTime = m.traceData.Events[m.currentEventIndex].TimeValue
					m.updateClusterState()
				}
			}

		case "esc":
			// Clear search highlighting
			if m.searchActive {
				m.searchActive = false
				m.searchPattern = ""
				return m, nil
			}
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	}

	return m, nil
}

// formatRoleLabel formats a role label with ID and epoch (if applicable)
// For TLog, LogRouter, BackupWorker: shows "RoleName [ID] (e=Epoch)"
// For other roles: shows "RoleName [ID]" or just "RoleName"
func formatRoleLabel(role RoleInfo) string {
	roleLabel := role.Name
	if role.ID != "" {
		roleLabel = fmt.Sprintf("%s [%s]", role.Name, role.ID)
	}
	// Add epoch for TLog, LogRouter, and BackupWorker roles
	if role.Epoch != "" && (role.Name == "TLog" || role.Name == "LogRouter" || role.Name == "BackupWorker") {
		roleLabel = fmt.Sprintf("%s (e=%s)", roleLabel, role.Epoch)
	}
	return roleLabel
}

// formatNumberWithCommas formats an int64 with comma separators for readability
// e.g., 53407865 -> "53,407,865"
func formatNumberWithCommas(n int64) string {
	if n < 0 {
		return "-" + formatNumberWithCommas(-n)
	}
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	// Insert commas from right to left
	var result strings.Builder
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// formatTraceEvent formats a trace event for display with config.fish field ordering and colors
func formatTraceEvent(event *TraceEvent, isCurrent bool, searchPattern string) string {
	// Skip fields as per config.fish and fields shown in topology
	skipFields := map[string]bool{
		"DateTime":         true,
		"ThreadID":         true,
		"LogGroup":         true,
		"TrackLatestType":  true,
		"Roles":            true, // Shown in topology
	}

	// Color styles
	fieldNameStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("240")) // Dim
	fieldValueStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("46"))  // Green
	currentLineStyle := lipgloss.NewStyle().Background(lipgloss.Color("58")) // Dark yellowish highlight
	searchHighlightStyle := lipgloss.NewStyle().Background(lipgloss.Color("58")) // Same as current line highlight

	var parts []string

	// Compile regex for search if pattern provided
	var searchRe *regexp.Regexp
	if searchPattern != "" {
		regexPattern := convertWildcardToRegex(searchPattern)
		searchRe, _ = regexp.Compile(regexPattern)
	}

	// Helper function to apply search highlighting to a field
	applySearchHighlight := func(text string) string {
		if searchRe == nil {
			return text
		}

		// Extract literal parts from the search pattern (between wildcards)
		literals := extractLiterals(searchPattern)
		if len(literals) == 0 {
			return text
		}

		// Collect all match positions for all literals
		type matchPos struct {
			start int
			end   int
		}
		var allMatches []matchPos

		for _, literal := range literals {
			if literal == "" {
				continue
			}
			// Create a regex for this literal (case-sensitive, escaped)
			literalPattern := regexp.QuoteMeta(literal)
			literalRe, err := regexp.Compile(literalPattern)
			if err != nil {
				continue
			}

			// Find all occurrences of this literal
			matches := literalRe.FindAllStringIndex(text, -1)
			for _, match := range matches {
				allMatches = append(allMatches, matchPos{start: match[0], end: match[1]})
			}
		}

		if len(allMatches) == 0 {
			return text
		}

		// Sort matches by start position
		sort.Slice(allMatches, func(i, j int) bool {
			return allMatches[i].start < allMatches[j].start
		})

		// Merge overlapping matches
		var merged []matchPos
		for _, match := range allMatches {
			if len(merged) == 0 {
				merged = append(merged, match)
			} else {
				last := &merged[len(merged)-1]
				if match.start <= last.end {
					// Overlapping or adjacent, merge them
					if match.end > last.end {
						last.end = match.end
					}
				} else {
					// Non-overlapping, add as new
					merged = append(merged, match)
				}
			}
		}

		// Build highlighted string
		var result strings.Builder
		lastEnd := 0
		for _, match := range merged {
			// Add text before match
			if match.start > lastEnd {
				result.WriteString(text[lastEnd:match.start])
			}
			// Add highlighted match
			result.WriteString(searchHighlightStyle.Render(text[match.start:match.end]))
			lastEnd = match.end
		}
		// Add remaining text
		if lastEnd < len(text) {
			result.WriteString(text[lastEnd:])
		}
		return result.String()
	}

	// Add fields in specific order: Time, Type, Severity (skip Machine, Roles, ID - shown in topology), then other attributes
	if event.Time != "" {
		parts = append(parts, fieldNameStyle.Render("Time=")+fieldValueStyle.Render(applySearchHighlight(event.Time)))
	}
	if event.Type != "" {
		parts = append(parts, fieldNameStyle.Render("Type=")+fieldValueStyle.Render(applySearchHighlight(event.Type)))
	}
	if event.Severity != "" {
		parts = append(parts, fieldNameStyle.Render("Severity=")+fieldValueStyle.Render(applySearchHighlight(event.Severity)))
	}
	// Skip Machine - shown in topology
	// Skip Roles - shown in topology
	// Skip ID - shown in topology

	// Add remaining attributes (sorted for consistent ordering)
	var attrKeys []string
	for key := range event.Attrs {
		if key == "Roles" {
			continue // Already handled
		}
		if skipFields[key] {
			continue
		}
		attrKeys = append(attrKeys, key)
	}
	// Sort keys for deterministic ordering
	sort.Strings(attrKeys)

	for _, key := range attrKeys {
		value := event.Attrs[key]
		parts = append(parts, fieldNameStyle.Render(key+"=")+lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Render(applySearchHighlight(value)))
	}

	line := strings.Join(parts, " ")
	if isCurrent {
		return currentLineStyle.Render(line)
	}
	return line
}

// handleMachineSelectionPopup handles keys in the machine selection popup
func (m model) handleMachineSelectionPopup(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "ctrl+c", "q":
			// Exit machine selection
			m.filterMachineSelectMode = false
			m.filterMachineInput.Reset()
			m.filterMachineInput.Blur()
			return m, nil

		case "enter":
			// Confirm selection and return to filter view
			m.filterMachineSelectMode = false
			m.filterMachineInput.Reset()
			m.filterMachineInput.Blur()
			return m, nil

		case "ctrl+n":
			// Move down in current column
			// We need to recalculate columns to know the size
			items := m.getMachineSelectionItems()
			maxDisplayLines := m.height - 12
			var columns [][]SelectableItem
			currentColumn := []SelectableItem{}
			currentHeight := 0

			for i := 0; i < len(items); i++ {
				item := items[i]
				itemLines := 1
				if item.Type == "machine" && item.Worker != nil {
					itemLines += len(item.Worker.Roles)
				}
				if currentHeight+itemLines > maxDisplayLines && len(currentColumn) > 0 {
					columns = append(columns, currentColumn)
					currentColumn = []SelectableItem{}
					currentHeight = 0
				}
				currentColumn = append(currentColumn, item)
				currentHeight += itemLines
			}
			if len(currentColumn) > 0 {
				columns = append(columns, currentColumn)
			}

			// Clamp column index
			if m.filterMachineColumn >= len(columns) {
				m.filterMachineColumn = len(columns) - 1
			}
			if m.filterMachineColumn < 0 && len(columns) > 0 {
				m.filterMachineColumn = 0
			}

			// Get current column
			var displayItems []SelectableItem
			if m.filterMachineColumn >= 0 && m.filterMachineColumn < len(columns) {
				displayItems = columns[m.filterMachineColumn]
			}

			// Increment selection within current column
			if len(displayItems) > 0 {
				m.filterMachineSelected++
				if m.filterMachineSelected >= len(displayItems) {
					m.filterMachineSelected = 0 // Wrap around
				}
			}
			return m, nil

		case "ctrl+p":
			// Move up in current column
			// We need to recalculate columns to know the size
			items := m.getMachineSelectionItems()
			maxDisplayLines := m.height - 12
			var columns [][]SelectableItem
			currentColumn := []SelectableItem{}
			currentHeight := 0

			for i := 0; i < len(items); i++ {
				item := items[i]
				itemLines := 1
				if item.Type == "machine" && item.Worker != nil {
					itemLines += len(item.Worker.Roles)
				}
				if currentHeight+itemLines > maxDisplayLines && len(currentColumn) > 0 {
					columns = append(columns, currentColumn)
					currentColumn = []SelectableItem{}
					currentHeight = 0
				}
				currentColumn = append(currentColumn, item)
				currentHeight += itemLines
			}
			if len(currentColumn) > 0 {
				columns = append(columns, currentColumn)
			}

			// Clamp column index
			if m.filterMachineColumn >= len(columns) {
				m.filterMachineColumn = len(columns) - 1
			}
			if m.filterMachineColumn < 0 && len(columns) > 0 {
				m.filterMachineColumn = 0
			}

			// Get current column
			var displayItems []SelectableItem
			if m.filterMachineColumn >= 0 && m.filterMachineColumn < len(columns) {
				displayItems = columns[m.filterMachineColumn]
			}

			// Decrement selection within current column
			if len(displayItems) > 0 {
				m.filterMachineSelected--
				if m.filterMachineSelected < 0 {
					m.filterMachineSelected = len(displayItems) - 1 // Wrap around
				}
			}
			return m, nil

		case "ctrl+f":
			// Move to next column (forward)
			items := m.getMachineSelectionItems()
			maxDisplayLines := m.height - 12
			var columns [][]SelectableItem
			currentColumn := []SelectableItem{}
			currentHeight := 0

			for i := 0; i < len(items); i++ {
				item := items[i]
				itemLines := 1
				if item.Type == "machine" && item.Worker != nil {
					itemLines += len(item.Worker.Roles)
				}
				if currentHeight+itemLines > maxDisplayLines && len(currentColumn) > 0 {
					columns = append(columns, currentColumn)
					currentColumn = []SelectableItem{}
					currentHeight = 0
				}
				currentColumn = append(currentColumn, item)
				currentHeight += itemLines
			}
			if len(currentColumn) > 0 {
				columns = append(columns, currentColumn)
			}

			// Move to next column
			if len(columns) > 0 {
				m.filterMachineColumn++
				if m.filterMachineColumn >= len(columns) {
					m.filterMachineColumn = 0 // Wrap to first column
				}
				// Reset selection to first item in new column
				m.filterMachineSelected = 0
			}
			return m, nil

		case "ctrl+b":
			// Move to previous column (backward)
			items := m.getMachineSelectionItems()
			maxDisplayLines := m.height - 12
			var columns [][]SelectableItem
			currentColumn := []SelectableItem{}
			currentHeight := 0

			for i := 0; i < len(items); i++ {
				item := items[i]
				itemLines := 1
				if item.Type == "machine" && item.Worker != nil {
					itemLines += len(item.Worker.Roles)
				}
				if currentHeight+itemLines > maxDisplayLines && len(currentColumn) > 0 {
					columns = append(columns, currentColumn)
					currentColumn = []SelectableItem{}
					currentHeight = 0
				}
				currentColumn = append(currentColumn, item)
				currentHeight += itemLines
			}
			if len(currentColumn) > 0 {
				columns = append(columns, currentColumn)
			}

			// Move to previous column
			if len(columns) > 0 {
				m.filterMachineColumn--
				if m.filterMachineColumn < 0 {
					m.filterMachineColumn = len(columns) - 1 // Wrap to last column
				}
				// Reset selection to first item in new column
				m.filterMachineSelected = 0
			}
			return m, nil

		case " ", "space":
			// Toggle selection of current item in current column
			// Recalculate columns to get current item
			items := m.getMachineSelectionItems()
			maxDisplayLines := m.height - 12
			var columns [][]SelectableItem
			currentColumn := []SelectableItem{}
			currentHeight := 0

			for i := 0; i < len(items); i++ {
				item := items[i]
				itemLines := 1
				if item.Type == "machine" && item.Worker != nil {
					itemLines += len(item.Worker.Roles)
				}
				if currentHeight+itemLines > maxDisplayLines && len(currentColumn) > 0 {
					columns = append(columns, currentColumn)
					currentColumn = []SelectableItem{}
					currentHeight = 0
				}
				currentColumn = append(currentColumn, item)
				currentHeight += itemLines
			}
			if len(currentColumn) > 0 {
				columns = append(columns, currentColumn)
			}

			// Clamp column index
			if m.filterMachineColumn >= len(columns) {
				m.filterMachineColumn = len(columns) - 1
			}
			if m.filterMachineColumn < 0 && len(columns) > 0 {
				m.filterMachineColumn = 0
			}

			// Get current column and item
			var displayItems []SelectableItem
			if m.filterMachineColumn >= 0 && m.filterMachineColumn < len(columns) {
				displayItems = columns[m.filterMachineColumn]
			}

			if m.filterMachineSelected >= 0 && m.filterMachineSelected < len(displayItems) {
				item := displayItems[m.filterMachineSelected]

				if item.Type == "dc" && item.DC != "Testers" {
					// Toggle DC selection
					m.filterMachineDCs[item.DC] = !m.filterMachineDCs[item.DC]
				} else if item.Type == "machine" {
					// Toggle individual machine selection
					found := false
					for i, m2 := range m.filterMachineList {
						if m2 == item.Machine {
							// Remove it
							m.filterMachineList = append(m.filterMachineList[:i], m.filterMachineList[i+1:]...)
							found = true
							break
						}
					}
					if !found {
						m.filterMachineList = append(m.filterMachineList, item.Machine)
					}
					// Rebuild machine set for O(1) lookups
					m.rebuildMachineSet()
				}
			}
			return m, nil

		default:
			// Pass other keys (except space) to input for fuzzy search
			// Reset selection and column when search changes
			oldValue := m.filterMachineInput.Value()
			m.filterMachineInput, cmd = m.filterMachineInput.Update(msg)
			if m.filterMachineInput.Value() != oldValue {
				m.filterMachineSelected = 0
				m.filterMachineColumn = 0 // Also reset to first column
			}
			return m, cmd
		}
	}
	return m, nil
}

// handleFilterTimeInput handles time range input
func (m model) handleFilterTimeInput(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "ctrl+c":
			// Cancel time input
			m.filterTimeInputMode = false
			m.filterTimeInput.Reset()
			m.filterTimeInput.Blur()
			return m, nil

		case "enter":
			// Save the time value
			if timeStr := m.filterTimeInput.Value(); timeStr != "" {
				if t, err := strconv.ParseFloat(timeStr, 64); err == nil {
					if m.filterTimeEditingStart {
						m.filterTimeStart = t
						// Move to editing end time
						m.filterTimeEditingStart = false
						m.filterTimeInput.SetValue(fmt.Sprintf("%.6f", m.filterTimeEnd))
						return m, nil
					} else {
						m.filterTimeEnd = t
						// Enable filter and exit
						m.filterTimeEnabled = true
						m.filterTimeInputMode = false
						m.filterTimeInput.Reset()
						m.filterTimeInput.Blur()
						return m, nil
					}
				}
			}
			return m, nil

		case "tab":
			// Toggle between start and end
			if timeStr := m.filterTimeInput.Value(); timeStr != "" {
				if t, err := strconv.ParseFloat(timeStr, 64); err == nil {
					if m.filterTimeEditingStart {
						m.filterTimeStart = t
					} else {
						m.filterTimeEnd = t
					}
				}
			}
			m.filterTimeEditingStart = !m.filterTimeEditingStart
			if m.filterTimeEditingStart {
				m.filterTimeInput.SetValue(fmt.Sprintf("%.6f", m.filterTimeStart))
			} else {
				m.filterTimeInput.SetValue(fmt.Sprintf("%.6f", m.filterTimeEnd))
			}
			return m, nil

		default:
			m.filterTimeInput, cmd = m.filterTimeInput.Update(msg)
			return m, cmd
		}
	}
	return m, nil
}

// handleTypeSearchPopup handles the Type search popup for fuzzy matching Type values
func (m model) handleTypeSearchPopup(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "ctrl+c":
			// Cancel Type search
			m.filterTypeSearchMode = false
			m.filterTypeSearchInput.Reset()
			m.filterTypeSearchInput.Blur()
			m.filterTypeSearchSelected = 0
			return m, nil

		case "enter":
			// Add selected Type as filter
			searchTerm := strings.ToLower(m.filterTypeSearchInput.Value())
			var filteredTypes []string
			for _, t := range m.filterTypeSearchList {
				if searchTerm == "" || strings.Contains(strings.ToLower(t), searchTerm) {
					filteredTypes = append(filteredTypes, t)
				}
			}

			if len(filteredTypes) > 0 && m.filterTypeSearchSelected >= 0 && m.filterTypeSearchSelected < len(filteredTypes) {
				selectedType := filteredTypes[m.filterTypeSearchSelected]
				// Add filter with trailing space for exact matching
				filterPattern := fmt.Sprintf("Type=%s ", selectedType)
				m.filterRawList = append(m.filterRawList, filterPattern)
				m.filterRawSelectedIndex = len(m.filterRawList) - 1
				// Update column index
				maxRows := 5
				m.filterRawColumn = m.filterRawSelectedIndex / maxRows
				// Recompile regex patterns for performance
				m.recompileRawFilterRegexes()
			}

			// Close popup
			m.filterTypeSearchMode = false
			m.filterTypeSearchInput.Reset()
			m.filterTypeSearchInput.Blur()
			m.filterTypeSearchSelected = 0
			// Ensure current event is visible after adding filter
			m.ensureCurrentEventVisible()
			return m, nil

		case "ctrl+n":
			// Move down in filtered list
			searchTerm := strings.ToLower(m.filterTypeSearchInput.Value())
			var filteredTypes []string
			for _, t := range m.filterTypeSearchList {
				if searchTerm == "" || strings.Contains(strings.ToLower(t), searchTerm) {
					filteredTypes = append(filteredTypes, t)
				}
			}

			if len(filteredTypes) > 0 {
				m.filterTypeSearchSelected++
				if m.filterTypeSearchSelected >= len(filteredTypes) {
					m.filterTypeSearchSelected = 0 // Wrap around
				}
			}
			return m, nil

		case "ctrl+p":
			// Move up in filtered list
			searchTerm := strings.ToLower(m.filterTypeSearchInput.Value())
			var filteredTypes []string
			for _, t := range m.filterTypeSearchList {
				if searchTerm == "" || strings.Contains(strings.ToLower(t), searchTerm) {
					filteredTypes = append(filteredTypes, t)
				}
			}

			if len(filteredTypes) > 0 {
				m.filterTypeSearchSelected--
				if m.filterTypeSearchSelected < 0 {
					m.filterTypeSearchSelected = len(filteredTypes) - 1 // Wrap around
				}
			}
			return m, nil

		default:
			// Pass other keys to input for fuzzy search
			// Reset selection when search changes
			oldValue := m.filterTypeSearchInput.Value()
			m.filterTypeSearchInput, cmd = m.filterTypeSearchInput.Update(msg)
			if m.filterTypeSearchInput.Value() != oldValue {
				m.filterTypeSearchSelected = 0
			}
			return m, cmd
		}
	}
	return m, nil
}

// getFilteredMachineList returns lists of DCs and machines filtered by search input
func (m model) getFilteredMachineList() ([]string, []string) {
	searchTerm := strings.ToLower(m.filterMachineInput.Value())

	// Collect all unique machines and DCs from trace data
	machineSet := make(map[string]bool)
	dcSet := make(map[string]bool)

	for _, event := range m.traceData.Events {
		if event.Machine != "" {
			machineSet[event.Machine] = true
			// Extract DC from machine address
			if dc := extractDCFromAddress(event.Machine); dc != "" {
				dcSet[dc] = true
			}
		}
	}

	// Convert to sorted slices
	var machines []string
	for machine := range machineSet {
		if searchTerm == "" || strings.Contains(strings.ToLower(machine), searchTerm) {
			machines = append(machines, machine)
		}
	}
	sort.Strings(machines)

	var dcs []string
	for dc := range dcSet {
		dcs = append(dcs, dc)
	}
	sort.Strings(dcs)

	return machines, dcs
}

// extractDCFromAddress extracts DC ID from machine address
func extractDCFromAddress(addr string) string {
	// Format: [abcd::X:Y:Z:W]:Port or X.Y.Z.W:Port
	// Y is the DC ID
	addr = strings.TrimPrefix(addr, "[")
	addr = strings.TrimSuffix(addr, "]")

	parts := strings.Split(addr, ":")
	if len(parts) >= 4 {
		// IPv6 format: abcd::X:Y:Z:W
		return parts[len(parts)-3]
	} else if len(parts) >= 1 {
		// IPv4 format: X.Y.Z.W
		dotParts := strings.Split(parts[0], ".")
		if len(dotParts) >= 2 {
			return dotParts[1]
		}
	}
	return ""
}

// SelectableItem represents an item in the machine selection popup
type SelectableItem struct {
	Type     string  // "dc" or "machine"
	DC       string  // DC ID for "dc" type
	Machine  string  // Machine address for "machine" type
	Worker   *Worker // Worker info for displaying roles
	IsTester bool
}

// getMachineSelectionItems builds the list of selectable items for machine selection popup
func (m model) getMachineSelectionItems() []SelectableItem {
	// Build machine topology similar to main view
	clusterState := BuildClusterState(m.traceData.Events)
	dcWorkers := clusterState.GetWorkersByDC()
	testers := clusterState.GetTesters()

	searchTerm := strings.ToLower(m.filterMachineInput.Value())

	var items []SelectableItem

	// Add DCs first
	var dcIDs []string
	for dcID := range dcWorkers {
		dcIDs = append(dcIDs, dcID)
	}
	sort.Strings(dcIDs)

	for _, dcID := range dcIDs {
		// Check if DC or any of its machines match search
		dcMatches := searchTerm == "" || strings.Contains(strings.ToLower("dc"+dcID), searchTerm)
		machineMatches := false

		if !dcMatches {
			// Check if any machine in this DC matches
			for _, worker := range dcWorkers[dcID] {
				if strings.Contains(strings.ToLower(worker.Machine), searchTerm) {
					machineMatches = true
					break
				}
			}
		}

		if dcMatches || machineMatches {
			items = append(items, SelectableItem{Type: "dc", DC: dcID})
			// Add machines under this DC
			for _, worker := range dcWorkers[dcID] {
				// If DC matched, show all machines. Otherwise only show matching machines.
				if dcMatches || strings.Contains(strings.ToLower(worker.Machine), searchTerm) {
					items = append(items, SelectableItem{Type: "machine", Machine: worker.Machine, Worker: worker, IsTester: false})
				}
			}
		}
	}

	// Add testers
	if len(testers) > 0 {
		testersMatches := searchTerm == "" || strings.Contains(strings.ToLower("testers"), searchTerm)
		machineMatches := false

		if !testersMatches {
			// Check if any tester machine matches
			for _, worker := range testers {
				if strings.Contains(strings.ToLower(worker.Machine), searchTerm) {
					machineMatches = true
					break
				}
			}
		}

		if testersMatches || machineMatches {
			items = append(items, SelectableItem{Type: "dc", DC: "Testers"})
			for _, worker := range testers {
				// If "Testers" matched, show all. Otherwise only show matching machines.
				if testersMatches || strings.Contains(strings.ToLower(worker.Machine), searchTerm) {
					items = append(items, SelectableItem{Type: "machine", Machine: worker.Machine, Worker: worker, IsTester: true})
				}
			}
		}
	}

	return items
}


func (m *model) updateClusterState() {
	// Get events up to and including the current event index
	events := m.traceData.Events[:m.currentEventIndex+1]
	m.clusterState = BuildClusterState(events)
}

// ensureCurrentEventVisible makes sure the current event index points to a visible event
// If current event is filtered out, find the nearest visible event
func (m *model) ensureCurrentEventVisible() {
	// Check if current event is visible
	if m.currentEventIndex >= 0 && m.currentEventIndex < len(m.traceData.Events) {
		if eventMatchesFilters(&m.traceData.Events[m.currentEventIndex], m) {
			// Current event is visible, nothing to do
			return
		}
	}

	// Current event is filtered out, find nearest visible event
	// Try forward first
	for i := m.currentEventIndex + 1; i < len(m.traceData.Events); i++ {
		if eventMatchesFilters(&m.traceData.Events[i], m) {
			m.currentEventIndex = i
			m.currentTime = m.traceData.Events[i].TimeValue
			m.updateClusterState()
			return
		}
	}

	// Try backward
	for i := m.currentEventIndex - 1; i >= 0; i-- {
		if eventMatchesFilters(&m.traceData.Events[i], m) {
			m.currentEventIndex = i
			m.currentTime = m.traceData.Events[i].TimeValue
			m.updateClusterState()
			return
		}
	}

	// No visible events found, stay at current (but it won't be visible)
}

// buildEventListPane builds the event list pane showing events around current time
func (m model) buildEventListPane(availableHeight int, paneWidth int, searchPattern string) []string {
	var lines []string
	currentIdx := m.currentEventIndex

	// We need to center based on LINE count, not event count
	// First, render the current event to see how many lines it takes
	currentEvent := &m.traceData.Events[currentIdx]
	currentEventLine := formatTraceEvent(currentEvent, false, searchPattern)
	currentWrappedLines := wrapText(currentEventLine, paneWidth)
	currentEventLineCount := len(currentWrappedLines)

	// Calculate how many lines we want above and below current event
	targetLinesAbove := availableHeight / 2

	// Build lines going backwards from current event
	var linesAbove []string
	lineCount := 0
	for i := currentIdx - 1; i >= 0 && lineCount < targetLinesAbove; i-- {
		event := &m.traceData.Events[i]

		// Skip filtered events
		if !eventMatchesFilters(event, &m) {
			continue
		}

		eventLine := formatTraceEvent(event, false, "") // No highlighting for non-current events
		wrappedLines := wrapText(eventLine, paneWidth)

		// Check if adding this event would exceed our target
		if lineCount+len(wrappedLines) > targetLinesAbove {
			break
		}

		// Prepend to linesAbove (we're going backwards)
		for j := len(wrappedLines) - 1; j >= 0; j-- {
			linesAbove = append([]string{wrappedLines[j]}, linesAbove...)
		}
		lineCount += len(wrappedLines)
	}

	// Add lines above
	lines = append(lines, linesAbove...)

	// Add current event with highlight (only first line)
	highlightStyle := lipgloss.NewStyle().Background(lipgloss.Color("58"))
	for i, line := range currentWrappedLines {
		if i == 0 {
			// Highlight only the first line (where Time= appears)
			lines = append(lines, highlightStyle.Render(line))
		} else {
			// Subsequent wrapped lines are not highlighted
			lines = append(lines, line)
		}
	}

	// Build lines going forwards from current event
	lineCount = len(linesAbove) + currentEventLineCount
	for i := currentIdx + 1; i < len(m.traceData.Events) && lineCount < availableHeight; i++ {
		event := &m.traceData.Events[i]

		// Skip filtered events
		if !eventMatchesFilters(event, &m) {
			continue
		}

		eventLine := formatTraceEvent(event, false, "") // No highlighting for non-current events
		wrappedLines := wrapText(eventLine, paneWidth)

		// Check if adding this event would exceed available height
		if lineCount+len(wrappedLines) > availableHeight {
			break
		}

		for _, line := range wrappedLines {
			lines = append(lines, line)
		}
		lineCount += len(wrappedLines)
	}

	return lines
}

// wrapText wraps text to fit within the specified width, preserving ANSI color codes
func wrapText(text string, width int) []string {
	if width <= 0 {
		return []string{text}
	}

	// Use lipgloss to wrap text
	wrapStyle := lipgloss.NewStyle().Width(width)
	wrapped := wrapStyle.Render(text)

	// Split into lines
	lines := strings.Split(wrapped, "\n")
	return lines
}


// View renders the UI (required by Bubbletea)
func (m model) View() string {
	// Get current event's machine and ID for highlighting in topology
	var currentMachine string
	var currentID string
	var currentEvent *TraceEvent
	if m.currentEventIndex >= 0 && m.currentEventIndex < len(m.traceData.Events) {
		currentEvent = &m.traceData.Events[m.currentEventIndex]
		currentMachine = currentEvent.Machine
		currentID = currentEvent.ID
	}

	// Calculate available height for topology (reserve space for config, recovery, scrubber, help)
	// Bottom section (with borders and padding):
	//   - configStyle border + padding + content = 3 lines
	//   - recovery line = 1 line
	//   - scrubberStyle border + padding + content = 3 lines
	//   - help line = 1 line
	//   - epoch info line = 1 line
	//   Total bottom = 9 lines
	availableHeight := m.height - 9
	if availableHeight < 1 {
		availableHeight = 1 // Minimum 1 line
	}

	// Styles
	dcHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("33")).
		Underline(true).
		MarginTop(0).
		MarginBottom(0)

	testerHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("135")).
		Underline(true).
		MarginTop(0).
		MarginBottom(0)

	workerStyleGray := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240")).
		PaddingLeft(2)

	workerStyleGreen := lipgloss.NewStyle().
		Foreground(lipgloss.Color("46")).
		Bold(true).
		PaddingLeft(2)

	// Style for current machine (event source) - cyan with arrow
	workerStyleCurrent := lipgloss.NewStyle().
		Foreground(lipgloss.Color("51")).
		Bold(true).
		PaddingLeft(0)

	roleStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252")) // Normal gray color

	// Style for current role (when ID matches)
	roleStyleCurrent := lipgloss.NewStyle().
		Foreground(lipgloss.Color("51")).
		Bold(true)

	scrubberStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241")).
		PaddingLeft(1)

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241"))

	// Get workers grouped by DC and testers
	dcWorkers := m.clusterState.GetWorkersByDC()
	testers := m.clusterState.GetTesters()

	// Extract NetworkMessageSent info if current event is a network message
	var networkMsg *struct {
		SrcAddr     string
		DstAddr     string
		RPCName     string
		StrippedRPC string
		SrcExists   bool // Whether source machine exists in topology
		DstExists   bool // Whether destination machine exists in topology
	}
	if currentEvent != nil && currentEvent.Type == "NetworkMessageSent" {
		srcAddr := currentEvent.Attrs["SrcAddr"]
		dstAddr := currentEvent.Attrs["DstAddr"]
		rpcName := currentEvent.Attrs["RPCName"]
		if srcAddr != "" && dstAddr != "" && rpcName != "" {
			// Normalize addresses to handle :tls suffix inconsistency
			normalizedSrc := normalizeAddress(srcAddr)
			normalizedDst := normalizeAddress(dstAddr)

			// Check if src and dst exist in topology
			srcExists := false
			dstExists := false

			// Check in dcWorkers
			for _, workers := range dcWorkers {
				for _, worker := range workers {
					normalizedWorker := normalizeAddress(worker.Machine)
					if normalizedWorker == normalizedSrc {
						srcExists = true
					}
					if normalizedWorker == normalizedDst {
						dstExists = true
					}
				}
			}

			// Check in testers
			for _, worker := range testers {
				normalizedWorker := normalizeAddress(worker.Machine)
				if normalizedWorker == normalizedSrc {
					srcExists = true
				}
				if normalizedWorker == normalizedDst {
					dstExists = true
				}
			}

			networkMsg = &struct {
				SrcAddr     string
				DstAddr     string
				RPCName     string
				StrippedRPC string
				SrcExists   bool
				DstExists   bool
			}{
				SrcAddr:     srcAddr,
				DstAddr:     dstAddr,
				RPCName:     rpcName,
				StrippedRPC: stripRPCName(rpcName),
				SrcExists:   srcExists,
				DstExists:   dstExists,
			}
		}
	}

	// Build all topology lines first (before packing into columns)
	var allTopologyLines []string

	if len(dcWorkers) == 0 && len(testers) == 0 {
		allTopologyLines = append(allTopologyLines, "")
		allTopologyLines = append(allTopologyLines, "  <NO_CLUSTER_YET>")
	} else {
		// Display main machines grouped by DC
		if len(dcWorkers) > 0 {
			// Sort DC IDs for consistent display
			dcIDs := make([]string, 0, len(dcWorkers))
			for dcID := range dcWorkers {
				dcIDs = append(dcIDs, dcID)
			}
			// Simple sort
			for i := 0; i < len(dcIDs); i++ {
				for j := i + 1; j < len(dcIDs); j++ {
					if dcIDs[i] > dcIDs[j] {
						dcIDs[i], dcIDs[j] = dcIDs[j], dcIDs[i]
					}
				}
			}

			// Display each DC
			for _, dcID := range dcIDs {
				workers := dcWorkers[dcID]
				allTopologyLines = append(allTopologyLines, dcHeaderStyle.Render(fmt.Sprintf("DC%s", dcID)))

				for _, worker := range workers {
					// Check if this worker's machine matches current event
					isCurrentMachine := worker.Machine == currentMachine

					// Check if this worker's machine is the source of a network message
					// Normalize addresses to handle :tls suffix inconsistency
					isNetworkSrc := networkMsg != nil && normalizeAddress(worker.Machine) == normalizeAddress(networkMsg.SrcAddr)

					// Check if this worker's machine is the destination of a network message
					isNetworkDst := networkMsg != nil && normalizeAddress(worker.Machine) == normalizeAddress(networkMsg.DstAddr)

					// Check if any role ID matches current event ID
					var matchingRole *RoleInfo
					if isCurrentMachine && currentID != "" {
						for i := range worker.Roles {
							if worker.Roles[i].ID == currentID {
								matchingRole = &worker.Roles[i]
								break
							}
						}
					}

					// Display machine address at top level (no ID here)
					if matchingRole != nil {
						// Role ID matches - show machine normally, will highlight role below
						workerLine := fmt.Sprintf("● %s", worker.Machine)

						// Highlight network message src/dst with yellow background and directional arrow
						if isNetworkSrc {
							// Source: yellow background with →→→ at end
							networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
							workerLine = fmt.Sprintf("● %s →→→", worker.Machine)
							allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
						} else if isNetworkDst {
							// Destination: yellow background with ←←← at end
							networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
							workerLine = fmt.Sprintf("● %s ←←←", worker.Machine)
							allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
						} else if worker.HasNonWorkerRoles() {
							allTopologyLines = append(allTopologyLines, workerStyleGreen.Render(workerLine))
						} else {
							allTopologyLines = append(allTopologyLines, workerStyleGray.Render(workerLine))
						}

						// Show each role, highlighting the one with matching ID
						for _, role := range worker.Roles {
							roleLabel := formatRoleLabel(role)
							if role.ID == currentID {
								// Highlight this specific role
								allTopologyLines = append(allTopologyLines, roleStyleCurrent.Render("    → "+roleLabel))
							} else {
								allTopologyLines = append(allTopologyLines, roleStyle.Render("      "+roleLabel))
							}
						}
					} else if isCurrentMachine {
						// Machine matches but no role ID match - highlight machine
						workerLine := fmt.Sprintf("● %s", worker.Machine)

						// Network message highlighting takes precedence
						if isNetworkSrc {
							networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
							workerLine = fmt.Sprintf("● %s →→→", worker.Machine)
							allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
						} else if isNetworkDst {
							networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
							workerLine = fmt.Sprintf("● %s ←←←", worker.Machine)
							allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
						} else {
							allTopologyLines = append(allTopologyLines, workerStyleCurrent.Render("→ "+workerLine))
						}

						// Show all roles normally
						for _, role := range worker.Roles {
							roleLabel := formatRoleLabel(role)
							allTopologyLines = append(allTopologyLines, roleStyle.Render("      "+roleLabel))
						}
					} else if worker.HasNonWorkerRoles() {
						// Normal worker with roles (no match)
						workerLine := fmt.Sprintf("● %s", worker.Machine)

						if isNetworkSrc {
							networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
							workerLine = fmt.Sprintf("● %s →→→", worker.Machine)
							allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
						} else if isNetworkDst {
							networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
							workerLine = fmt.Sprintf("● %s ←←←", worker.Machine)
							allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
						} else {
							allTopologyLines = append(allTopologyLines, workerStyleGreen.Render(workerLine))
						}

						for _, role := range worker.Roles {
							roleLabel := formatRoleLabel(role)
							allTopologyLines = append(allTopologyLines, roleStyle.Render("      "+roleLabel))
						}
					} else {
						// Worker without roles OR only Worker role (no match)
						workerLine := fmt.Sprintf("● %s", worker.Machine)

						if isNetworkSrc {
							networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
							workerLine = fmt.Sprintf("● %s →→→", worker.Machine)
							allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
						} else if isNetworkDst {
							networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
							workerLine = fmt.Sprintf("● %s ←←←", worker.Machine)
							allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
						} else {
							allTopologyLines = append(allTopologyLines, workerStyleGray.Render(workerLine))
						}

						// Show all roles (including Worker if present)
						for _, role := range worker.Roles {
							roleLabel := formatRoleLabel(role)
							allTopologyLines = append(allTopologyLines, roleStyle.Render("      "+roleLabel))
						}
					}
				}
			}
		}

		// Display testers in a separate section
		if len(testers) > 0 {
			allTopologyLines = append(allTopologyLines, testerHeaderStyle.Render("Testers"))

			for _, worker := range testers {
				// Check if this tester's machine matches current event
				isCurrentMachine := worker.Machine == currentMachine

				// Check if this worker's machine is the source of a network message
				// Normalize addresses to handle :tls suffix inconsistency
				isNetworkSrc := networkMsg != nil && normalizeAddress(worker.Machine) == normalizeAddress(networkMsg.SrcAddr)

				// Check if this worker's machine is the destination of a network message
				isNetworkDst := networkMsg != nil && normalizeAddress(worker.Machine) == normalizeAddress(networkMsg.DstAddr)

				// Check if any role ID matches current event ID
				var matchingRole *RoleInfo
				if isCurrentMachine && currentID != "" {
					for i := range worker.Roles {
						if worker.Roles[i].ID == currentID {
							matchingRole = &worker.Roles[i]
							break
						}
					}
				}

				// Display machine address at top level (no ID here)
				if matchingRole != nil {
					// Role ID matches - show machine normally, will highlight role below
					workerLine := fmt.Sprintf("● %s", worker.Machine)

					// Highlight network message src/dst with yellow background and directional arrow
					if isNetworkSrc {
						// Source: yellow background with →→→ at end
						networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
						workerLine = fmt.Sprintf("● %s →→→", worker.Machine)
						allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
					} else if isNetworkDst {
						// Destination: yellow background with ←←← at end
						networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
						workerLine = fmt.Sprintf("● %s ←←←", worker.Machine)
						allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
					} else if worker.HasNonWorkerRoles() {
						allTopologyLines = append(allTopologyLines, workerStyleGreen.Render(workerLine))
					} else {
						allTopologyLines = append(allTopologyLines, workerStyleGray.Render(workerLine))
					}

					// Show each role, highlighting the one with matching ID
					for _, role := range worker.Roles {
						roleLabel := formatRoleLabel(role)
						if role.ID == currentID {
							// Highlight this specific role
							allTopologyLines = append(allTopologyLines, roleStyleCurrent.Render("    → "+roleLabel))
						} else {
							allTopologyLines = append(allTopologyLines, roleStyle.Render("      "+roleLabel))
						}
					}
				} else if isCurrentMachine {
					// Machine matches but no role ID match - highlight machine
					workerLine := fmt.Sprintf("● %s", worker.Machine)

					// Network message highlighting takes precedence
					if isNetworkSrc {
						networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
						workerLine = fmt.Sprintf("● %s →→→", worker.Machine)
						allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
					} else if isNetworkDst {
						networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
						workerLine = fmt.Sprintf("● %s ←←←", worker.Machine)
						allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
					} else {
						allTopologyLines = append(allTopologyLines, workerStyleCurrent.Render("→ "+workerLine))
					}

					// Show all roles normally
					for _, role := range worker.Roles {
						roleLabel := formatRoleLabel(role)
						allTopologyLines = append(allTopologyLines, roleStyle.Render("      "+roleLabel))
					}
				} else if worker.HasNonWorkerRoles() {
					// Normal tester with roles (no match)
					workerLine := fmt.Sprintf("● %s", worker.Machine)

					if isNetworkSrc {
						networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
						workerLine = fmt.Sprintf("● %s →→→", worker.Machine)
						allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
					} else if isNetworkDst {
						networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
						workerLine = fmt.Sprintf("● %s ←←←", worker.Machine)
						allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
					} else {
						allTopologyLines = append(allTopologyLines, workerStyleGreen.Render(workerLine))
					}

					for _, role := range worker.Roles {
						roleLabel := formatRoleLabel(role)
						allTopologyLines = append(allTopologyLines, roleStyle.Render("      "+roleLabel))
					}
				} else {
					// Tester without roles OR only Worker role (no match)
					workerLine := fmt.Sprintf("● %s", worker.Machine)

					if isNetworkSrc {
						networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
						workerLine = fmt.Sprintf("● %s →→→", worker.Machine)
						allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
					} else if isNetworkDst {
						networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
						workerLine = fmt.Sprintf("● %s ←←←", worker.Machine)
						allTopologyLines = append(allTopologyLines, networkStyle.Render(workerLine))
					} else {
						allTopologyLines = append(allTopologyLines, workerStyleGray.Render(workerLine))
					}

					// Show all roles (including Worker if present)
					for _, role := range worker.Roles {
						roleLabel := formatRoleLabel(role)
						allTopologyLines = append(allTopologyLines, roleStyle.Render("      "+roleLabel))
					}
				}
			}
		}
	}

	// If we have a network message but source machine was not found in topology, show it separately
	if networkMsg != nil && !networkMsg.SrcExists {
		// Source not found, show with yellow highlight and →→→ arrow
		allTopologyLines = append(allTopologyLines, "")
		networkStyle := lipgloss.NewStyle().Background(lipgloss.Color("220")).Foreground(lipgloss.Color("0")).Bold(true)
		allTopologyLines = append(allTopologyLines, networkStyle.Render(fmt.Sprintf("● %s →→→", networkMsg.SrcAddr)))
	}

	// Add RPC name as a dedicated last row in topology section
	if networkMsg != nil && networkMsg.StrippedRPC != "" {
		allTopologyLines = append(allTopologyLines, "")
		rpcStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("220")).
			Bold(true).
			Underline(true)
		allTopologyLines = append(allTopologyLines, rpcStyle.Render(fmt.Sprintf("  RPC: %s", networkMsg.StrippedRPC)))
	}

	// Pack lines into columns based on available height
	// Keep machines and their roles together (don't split across columns)
	var columns [][]string
	if len(allTopologyLines) <= availableHeight {
		// Everything fits in one column
		columns = [][]string{allTopologyLines}
	} else {
		// Need multiple columns - group lines to keep machines together
		currentColumn := []string{}
		i := 0
		for i < len(allTopologyLines) {
			line := allTopologyLines[i]

			// Check if this is a DC header or machine line (starts with "DC", "Testers", "●", or "→")
			// These mark the start of a new logical group
			isGroupStart := strings.HasPrefix(line, "DC ") ||
				strings.HasPrefix(line, "Testers") ||
				strings.Contains(line, "● ") ||
				strings.Contains(line, "→ ●")

			if isGroupStart && len(currentColumn) > 0 {
				// Peek ahead to see how many lines this group needs
				groupSize := 1 // Current line
				for j := i + 1; j < len(allTopologyLines); j++ {
					nextLine := allTopologyLines[j]
					// Check if next line is a role (indented) or another group start
					if strings.HasPrefix(nextLine, "DC ") ||
						strings.HasPrefix(nextLine, "Testers") ||
						strings.Contains(nextLine, "● ") ||
						strings.Contains(nextLine, "→ ●") {
						break // End of this group
					}
					groupSize++
				}

				// If adding this group would overflow, start new column
				if len(currentColumn) + groupSize > availableHeight {
					columns = append(columns, currentColumn)
					currentColumn = []string{}
				}
			}

			currentColumn = append(currentColumn, line)
			i++
		}
		// Add the last column
		if len(currentColumn) > 0 {
			columns = append(columns, currentColumn)
		}
	}

	// Calculate max width for each column
	columnWidths := make([]int, len(columns))
	for colIdx, column := range columns {
		maxWidth := 0
		for _, line := range column {
			width := lipgloss.Width(line)
			if width > maxWidth {
				maxWidth = width
			}
		}
		columnWidths[colIdx] = maxWidth + 2 // Add 2 for spacing between columns
	}

	// Calculate total left pane width
	leftPaneWidth := 0
	for _, w := range columnWidths {
		leftPaneWidth += w
	}
	if leftPaneWidth < 20 {
		leftPaneWidth = 20
	}
	// Don't take more than 60% of screen
	maxLeftWidth := (m.width * 3) / 5
	if leftPaneWidth > maxLeftWidth {
		leftPaneWidth = maxLeftWidth
	}

	// Calculate right pane width (rest of screen minus border)
	rightPaneWidth := m.width - leftPaneWidth - 3 // 3 for " │ "
	if rightPaneWidth < 30 {
		rightPaneWidth = 30
	}

	// Build right pane (event list) content as array of lines
	// If in search mode, reserve 1 line for search bar
	eventListHeight := availableHeight
	if m.searchMode {
		eventListHeight = availableHeight - 1
	}

	// Pass search pattern if search is active
	searchPattern := ""
	if m.searchActive {
		searchPattern = m.searchPattern
	}
	eventLines := m.buildEventListPane(eventListHeight, rightPaneWidth, searchPattern)

	// If in search mode, add search bar as last line
	if m.searchMode {
		searchBarStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
		var searchBar string
		if m.searchDirection == "forward" {
			searchBar = "/" + m.searchInput.View()
		} else {
			searchBar = "?" + m.searchInput.View()
		}
		eventLines = append(eventLines, searchBarStyle.Render(searchBar))
	}

	// Pad columns to same height
	maxLines := availableHeight
	for i := range columns {
		for len(columns[i]) < maxLines {
			columns[i] = append(columns[i], "")
		}
	}
	for len(eventLines) < maxLines {
		eventLines = append(eventLines, "")
	}

	// Build split view line by line with columnar topology
	var splitContent strings.Builder
	borderStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("240"))

	for lineIdx := 0; lineIdx < maxLines; lineIdx++ {
		// Render all topology columns for this line
		for colIdx, column := range columns {
			var line string
			if lineIdx < len(column) {
				line = column[lineIdx]
			}

			// Pad to column width
			lineWidth := lipgloss.Width(line)
			targetWidth := columnWidths[colIdx]
			if lineWidth < targetWidth {
				line = line + strings.Repeat(" ", targetWidth-lineWidth)
			}

			splitContent.WriteString(line)
		}

		// Add border and event line
		splitContent.WriteString(borderStyle.Render(" │ "))

		if lineIdx < len(eventLines) {
			splitContent.WriteString(eventLines[lineIdx])
		}

		splitContent.WriteString("\n")
	}

	// Build bottom section (spans full width)
	var bottomSection strings.Builder

	// Add separator
	separator := strings.Repeat("─", m.width)
	bottomSection.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(separator))
	bottomSection.WriteString("\n")

	// DB Configuration section
	config := m.traceData.GetLatestConfigAtTime(m.currentTime)
	if config != nil {
		configStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("243")).
			PaddingLeft(1)

		configTitleStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("39")).
			Bold(true)

		configValueStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("252"))

		configContent := configTitleStyle.Render(fmt.Sprintf("DB Config (t=%.2fs)", config.Time)) + " "

		// Build compact config display with exact field names
		configParts := []string{}
		if config.RedundancyMode != "" {
			configParts = append(configParts, fmt.Sprintf("redundancy_mode=%s", config.RedundancyMode))
		}
		if config.UsableRegions > 0 {
			configParts = append(configParts, fmt.Sprintf("usable_regions=%d", config.UsableRegions))
		}
		if config.Logs > 0 {
			configParts = append(configParts, fmt.Sprintf("logs=%d", config.Logs))
		}
		if config.LogRouters > 0 {
			configParts = append(configParts, fmt.Sprintf("log_routers=%d", config.LogRouters))
		}
		if config.RemoteLogs > 0 {
			configParts = append(configParts, fmt.Sprintf("remote_logs=%d", config.RemoteLogs))
		}
		if config.Proxies > 0 {
			configParts = append(configParts, fmt.Sprintf("proxies=%d", config.Proxies))
		}
		if config.GrvProxies > 0 {
			configParts = append(configParts, fmt.Sprintf("grv_proxies=%d", config.GrvProxies))
		}
		if config.BackupWorkerEnabled > 0 {
			configParts = append(configParts, fmt.Sprintf("backup_worker_enabled=%d", config.BackupWorkerEnabled))
		}
		if config.StorageEngine != "" {
			configParts = append(configParts, fmt.Sprintf("storage_engine=%s", config.StorageEngine))
		}

		configContent += configValueStyle.Render(strings.Join(configParts, " | "))
		bottomSection.WriteString(configStyle.Render(configContent))
		bottomSection.WriteString("\n")
	}

	// Recovery State section
	recoveryState := m.traceData.GetLatestRecoveryStateAtIndex(m.currentEventIndex)
	if recoveryState != nil {
		recoveryStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("243")).
			PaddingLeft(1)

		recoveryTitleStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("39")).
			Bold(true)

		// Color code based on StatusCode value
		var recoveryValueStyle lipgloss.Style
		if statusCode, err := strconv.Atoi(recoveryState.StatusCode); err == nil {
			if statusCode < 11 {
				// Red for < 11
				recoveryValueStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
			} else if statusCode >= 11 && statusCode < 14 {
				// Blue for 11 <= statusCode < 14
				recoveryValueStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
			} else if statusCode == 14 {
				// Green for = 14
				recoveryValueStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("46"))
			} else {
				// Default gray for > 14
				recoveryValueStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
			}
		} else {
			// Default gray if can't parse
			recoveryValueStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
		}

		recoveryContent := recoveryTitleStyle.Render(fmt.Sprintf("Recovery State (t=%.6fs)", recoveryState.Time)) + " "
		recoveryContent += recoveryValueStyle.Render(fmt.Sprintf("StatusCode=%s | Status=%s", recoveryState.StatusCode, recoveryState.Status))

		bottomSection.WriteString(recoveryStyle.Render(recoveryContent))
		bottomSection.WriteString("\n")
	}

	// Epoch Version Info section
	epochInfo := m.traceData.GetLatestEpochVersionAtIndex(m.currentEventIndex)
	if epochInfo != nil {
		epochStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("243")).
			PaddingLeft(1)

		epochTitleStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("39")).
			Bold(true)

		epochValueStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")) // Same gray/white as config values

		// Format KCV - show "n/a" if not available
		kcvStr := "n/a"
		if epochInfo.HasKCV && epochInfo.KCV > 0 {
			kcvStr = formatNumberWithCommas(epochInfo.KCV)
		}

		epochContent := epochTitleStyle.Render(fmt.Sprintf("Epoch (t=%.6fs)", epochInfo.Time)) + " "
		epochContent += epochValueStyle.Render(fmt.Sprintf("epoch=%d | KCV=%s | RV=%s | recoveryTxnVersion=%s",
			epochInfo.Epoch, kcvStr, formatNumberWithCommas(epochInfo.RV), formatNumberWithCommas(epochInfo.RecoveryTxnVersion)))

		bottomSection.WriteString(epochStyle.Render(epochContent))
		bottomSection.WriteString("\n")
	}

	// Time scrubber
	separatorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	bottomSection.WriteString(separatorStyle.Render(strings.Repeat("─", 20)))
	bottomSection.WriteString("\n")
	scrubberContent := fmt.Sprintf("Time: %.6fs", m.currentTime)
	bottomSection.WriteString(scrubberStyle.Render(scrubberContent))
	bottomSection.WriteString("\n")

	// Help text
	help := helpStyle.Render("Ctrl+N/P: next/prev event | g/G: start/end | t: jump time | /?: search | n/N: next/prev match | f: filter | r/R: recovery | c: config | x: health | h: help | q: quit")
	bottomSection.WriteString(help)

	// Combine split view with bottom section
	fullView := splitContent.String() + bottomSection.String()

	// If in filter view mode, show appropriate popup overlay
	if m.filterViewMode {
		// If in machine selection sub-popup, show it
		if m.filterMachineSelectMode {
			return m.renderMachineSelectionPopup(fullView)
		}
		// If in time range configuration sub-popup, show it
		if m.filterTimeInputMode {
			return m.renderFilterTimeRangePopup(fullView)
		}
		// If in Type search sub-popup, show it
		if m.filterTypeSearchMode {
			return m.renderTypeSearchPopup(fullView)
		}
		// Otherwise show main filter popup
		return m.renderFilterPopup(fullView)
	}

	// If in help view mode, show help popup overlay
	if m.helpViewMode {
		return m.renderHelpPopup(fullView)
	}

	// If in health view mode, show health popup overlay
	if m.healthViewMode {
		return m.renderHealthPopup(fullView)
	}

	// If in config view mode, show config popup overlay
	if m.configViewMode {
		config := m.traceData.GetLatestConfigAtTime(m.currentTime)
		if config != nil {
			return m.renderConfigPopup(fullView, config)
		} else {
			// No config available yet - show message
			return m.renderNoConfigPopup(fullView)
		}
	}

	// If in time input mode, show popup overlay
	if m.timeInputMode {
		return m.renderTimeInputPopup(fullView)
	}

	return fullView
}

// renderFilterPopup renders the filter configuration popup overlay
func (m model) renderFilterPopup(baseView string) string {
	popupStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(1, 2).
		Width(90)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39")).
		Underline(true)

	categoryStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("46"))

	categorySelectedStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("226"))

	normalStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252"))

	grayedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240"))

	selectedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("226")).
		Bold(true)

	var content strings.Builder
	content.WriteString(titleStyle.Render("Filter Configuration"))
	content.WriteString("\n\n")

	// Show "All" checkbox
	checkbox := "[ ]"
	if m.filterShowAll {
		checkbox = "[x]"
	}
	content.WriteString(normalStyle.Render(fmt.Sprintf("%s All (space to toggle)", checkbox)))
	content.WriteString("\n")

	// Determine if categories should be grayed out
	isGrayed := m.filterShowAll

	// Category 1: Raw Filters
	content.WriteString("\n")
	cat1Style := categoryStyle
	if m.filterCurrentCategory == 0 && !isGrayed {
		cat1Style = categorySelectedStyle
	}
	if isGrayed {
		cat1Style = grayedStyle
	}
	content.WriteString(cat1Style.Render("[1] Raw Filters (OR within category):"))
	content.WriteString("\n")

	if isGrayed {
		content.WriteString(grayedStyle.Render("  (disabled - toggle All off to configure)"))
		content.WriteString("\n")
	} else {
		if len(m.filterRawList) == 0 {
			content.WriteString(normalStyle.Render("  (no filters)"))
			content.WriteString("\n")
		} else {
			// Pack filters into columns (max 5 rows per column)
			maxRows := 5
			numColumns := (len(m.filterRawList) + maxRows - 1) / maxRows

			// Build columns
			type ColumnItems struct {
				items      []int // Indices into m.filterRawList
				startIdx   int
				endIdx     int
			}
			var columns []ColumnItems
			for col := 0; col < numColumns; col++ {
				startIdx := col * maxRows
				endIdx := startIdx + maxRows
				if endIdx > len(m.filterRawList) {
					endIdx = len(m.filterRawList)
				}
				var items []int
				for i := startIdx; i < endIdx; i++ {
					items = append(items, i)
				}
				columns = append(columns, ColumnItems{
					items:    items,
					startIdx: startIdx,
					endIdx:   endIdx,
				})
			}

			// Show column indicator if multiple columns exist
			if len(columns) > 1 {
				columnIndicator := fmt.Sprintf("Column %d/%d (Ctrl+F/B to navigate)", m.filterRawColumn+1, len(columns))
				content.WriteString(normalStyle.Render("  " + columnIndicator))
				content.WriteString("\n\n")
			}

			// Display only current column
			if m.filterRawColumn >= 0 && m.filterRawColumn < len(columns) {
				currentColumn := columns[m.filterRawColumn]
				for _, i := range currentColumn.items {
					filter := m.filterRawList[i]
					filterStyle := normalStyle
					prefix := "  "

					// Check if this filter is disabled
					if m.filterRawDisabled[i] {
						filterStyle = grayedStyle
						filter = filter + " [disabled]"
					}

					// Check if selected
					if i == m.filterRawSelectedIndex && !m.filterRawInputActive {
						if m.filterRawDisabled[i] {
							// Selected but disabled - show in grayed selected style
							filterStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Bold(true)
						} else {
							filterStyle = selectedStyle
						}
						prefix = "→ "
					}

					content.WriteString(filterStyle.Render(fmt.Sprintf("%s%s", prefix, filter)))
					content.WriteString("\n")
				}
			}
		}
		// Input field for new raw filter
		if m.filterRawInputActive {
			content.WriteString(selectedStyle.Render("  New filter: "))
			content.WriteString(m.filterRawInput.View())
			content.WriteString("\n")
		} else {
			content.WriteString(normalStyle.Render("  Press 'a' to add, 'e' to edit, 'r' to remove, 'd' to toggle disable, 't' for Type search, 'c' for common, Ctrl+N/P to navigate, Ctrl+F/B for columns"))
			content.WriteString("\n")
		}
	}

	// Category 2: Machine Filters
	content.WriteString("\n")
	cat2Style := categoryStyle
	if m.filterCurrentCategory == 1 && !isGrayed {
		cat2Style = categorySelectedStyle
	}
	if isGrayed {
		cat2Style = grayedStyle
	}
	content.WriteString(cat2Style.Render("[2] By Machine (OR within category):"))
	content.WriteString("\n")

	if isGrayed {
		content.WriteString(grayedStyle.Render("  (disabled - toggle All off to configure)"))
		content.WriteString("\n")
	} else {
		// Calculate total number of selected machines (individual + from DCs)
		machineSet := make(map[string]bool)

		// Add individual machines
		for _, machine := range m.filterMachineList {
			machineSet[machine] = true
		}

		// Add machines from selected DCs using cluster state
		if len(m.filterMachineDCs) > 0 {
			// Build cluster state to get unique machines by DC
			clusterState := BuildClusterState(m.traceData.Events)
			dcWorkers := clusterState.GetWorkersByDC()

			for dcID, workers := range dcWorkers {
				if m.filterMachineDCs[dcID] {
					for _, worker := range workers {
						machineSet[worker.Machine] = true
					}
				}
			}
		}

		// Show selected machines count only (not DCs)
		if len(m.filterMachineList) == 0 && len(m.filterMachineDCs) == 0 {
			content.WriteString(normalStyle.Render("  (no machines selected)"))
			content.WriteString("\n")
		} else {
			content.WriteString(normalStyle.Render(fmt.Sprintf("  Machines: %d selected", len(machineSet))))
			content.WriteString("\n")
		}
		content.WriteString(normalStyle.Render("  Press Enter to configure"))
		content.WriteString("\n")
	}

	// Category 3: Time Range
	content.WriteString("\n")
	cat3Style := categoryStyle
	if m.filterCurrentCategory == 2 && !isGrayed {
		cat3Style = categorySelectedStyle
	}
	if isGrayed {
		cat3Style = grayedStyle
	}
	content.WriteString(cat3Style.Render("[3] By Time Range:"))
	content.WriteString("\n")

	if isGrayed {
		content.WriteString(grayedStyle.Render("  (disabled - toggle All off to configure)"))
		content.WriteString("\n")
	} else {
		if m.filterTimeEnabled {
			content.WriteString(normalStyle.Render(fmt.Sprintf("  [x] Enabled: %.6fs - %.6fs", m.filterTimeStart, m.filterTimeEnd)))
			content.WriteString("\n")
		} else {
			content.WriteString(normalStyle.Render("  [ ] Disabled"))
			content.WriteString("\n")
		}
		content.WriteString(normalStyle.Render("  Press 'd' to toggle, Enter to configure"))
		content.WriteString("\n")
	}

	// Category 4: Message Filter
	content.WriteString("\n")
	cat4Style := categoryStyle
	if m.filterCurrentCategory == 3 && !isGrayed {
		cat4Style = categorySelectedStyle
	}
	if isGrayed {
		cat4Style = grayedStyle
	}
	content.WriteString(cat4Style.Render("[4] By Message (NetworkMessageSent events only):"))
	content.WriteString("\n")

	if isGrayed {
		content.WriteString(grayedStyle.Render("  (disabled - toggle All off to configure)"))
		content.WriteString("\n")
	} else {
		if m.filterMessageEnabled {
			content.WriteString(normalStyle.Render("  [x] Enabled: Show only NetworkMessageSent events"))
			content.WriteString("\n")
		} else {
			content.WriteString(normalStyle.Render("  [ ] Disabled"))
			content.WriteString("\n")
		}
		content.WriteString(normalStyle.Render("  Press 'd' to toggle"))
		content.WriteString("\n")
	}

	// Help text
	content.WriteString("\n")
	if isGrayed {
		content.WriteString(normalStyle.Render("Space: toggle All | q/f/Esc: close"))
	} else {
		// Context-sensitive help based on current category
		var categoryHelp string
		if m.filterCurrentCategory == 0 {
			// Raw category
			categoryHelp = "a: add | t: Type search | e: edit | r: remove | d: toggle disable | c: common | Ctrl+N/P: navigate | Ctrl+F/B: columns | "
		} else if m.filterCurrentCategory == 1 {
			// Machine category
			categoryHelp = "Enter: configure machines | "
		} else if m.filterCurrentCategory == 2 {
			// Time category
			categoryHelp = "d: toggle | Enter: configure range | "
		} else if m.filterCurrentCategory == 3 {
			// Message category
			categoryHelp = "d: toggle | "
		}
		content.WriteString(normalStyle.Render(categoryHelp + "1/2/3/4: jump | Ctrl+N/P: switch | Space: toggle All | q/f/Esc: close"))
	}

	popup := popupStyle.Render(content.String())

	// Center the popup
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, popup, lipgloss.WithWhitespaceChars(" "))
}

// renderFilterTimeRangePopup renders the time range configuration popup
func (m model) renderFilterTimeRangePopup(baseView string) string {
	popupStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(1, 2).
		Width(60)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39"))

	labelStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252"))

	selectedLabelStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("226")).
		Bold(true)

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241")).
		MarginTop(1)

	rangeStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("243")).
		Italic(true)

	var content strings.Builder
	content.WriteString(titleStyle.Render("Configure Time Range Filter"))
	content.WriteString("\n\n")

	// Show which field is being edited
	startLabel := "Start Time:"
	endLabel := "End Time:"

	if m.filterTimeEditingStart {
		content.WriteString(selectedLabelStyle.Render(startLabel))
		content.WriteString(" ")
		content.WriteString(m.filterTimeInput.View())
		content.WriteString("\n\n")
		content.WriteString(labelStyle.Render(fmt.Sprintf("%s %.6fs", endLabel, m.filterTimeEnd)))
	} else {
		content.WriteString(labelStyle.Render(fmt.Sprintf("%s %.6fs", startLabel, m.filterTimeStart)))
		content.WriteString("\n\n")
		content.WriteString(selectedLabelStyle.Render(endLabel))
		content.WriteString(" ")
		content.WriteString(m.filterTimeInput.View())
	}

	content.WriteString("\n\n")
	rangeInfo := rangeStyle.Render(fmt.Sprintf("Valid range: %.2f - %.2f seconds", m.traceData.MinTime, m.traceData.MaxTime))
	content.WriteString(rangeInfo)

	content.WriteString("\n")
	content.WriteString(helpStyle.Render("Tab: switch field | Enter: confirm | Esc: cancel"))

	popup := popupStyle.Render(content.String())

	// Center the popup
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, popup, lipgloss.WithWhitespaceChars(" "))
}

// renderMachineSelectionPopup renders the machine selection popup overlay
func (m model) renderMachineSelectionPopup(baseView string) string {
	popupStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(1, 2).
		Width(90).
		MaxHeight(m.height - 4)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39")).
		Underline(true)

	dcHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("33"))

	testerHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("135"))

	normalStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252"))

	selectedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("226")).
		Bold(true)

	checkedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("46"))

	roleStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240"))

	var content strings.Builder
	content.WriteString(titleStyle.Render("Select Machines"))
	content.WriteString("\n\n")

	// Search input
	content.WriteString(normalStyle.Render("Search: "))
	content.WriteString(m.filterMachineInput.View())
	content.WriteString("\n\n")

	// Get items using helper function
	items := m.getMachineSelectionItems()

	// Calculate available height for content
	maxDisplayLines := m.height - 12 // Account for title, search, help, padding, border

	// Pack items into columns (keep DCs with their machines)
	var columns [][]SelectableItem
	currentColumn := []SelectableItem{}
	currentHeight := 0

	for i := 0; i < len(items); i++ {
		item := items[i]

		// Calculate how many lines this item will take
		itemLines := 1 // DC header or machine line
		if item.Type == "machine" && item.Worker != nil {
			itemLines += len(item.Worker.Roles) // Roles under machine
		}

		// If adding this item would overflow, start new column
		if currentHeight+itemLines > maxDisplayLines && len(currentColumn) > 0 {
			columns = append(columns, currentColumn)
			currentColumn = []SelectableItem{}
			currentHeight = 0
		}

		currentColumn = append(currentColumn, item)
		currentHeight += itemLines
	}

	// Add last column
	if len(currentColumn) > 0 {
		columns = append(columns, currentColumn)
	}

	// Clamp column index
	if m.filterMachineColumn >= len(columns) {
		m.filterMachineColumn = len(columns) - 1
	}
	if m.filterMachineColumn < 0 && len(columns) > 0 {
		m.filterMachineColumn = 0
	}

	// Display only current column
	if len(columns) == 0 {
		content.WriteString(normalStyle.Render("  (no matches)"))
		content.WriteString("\n")
	} else {
		// Get current column items
		var displayItems []SelectableItem
		if m.filterMachineColumn >= 0 && m.filterMachineColumn < len(columns) {
			displayItems = columns[m.filterMachineColumn]
		}

		// Show column indicator if multiple columns exist
		if len(columns) > 1 {
			columnIndicator := fmt.Sprintf("Column %d/%d (Ctrl+F/B to navigate)", m.filterMachineColumn+1, len(columns))
			content.WriteString(normalStyle.Render("  " + columnIndicator))
			content.WriteString("\n\n")
		}

		// Render items in current column
		for itemIdx, item := range displayItems {
			if item.Type == "dc" {
				var header string
				if item.DC == "Testers" {
					header = "Testers"
				} else {
					header = fmt.Sprintf("DC%s", item.DC)
				}

				// Check if this DC is selected (highlighted)
				if itemIdx == m.filterMachineSelected {
					content.WriteString(selectedStyle.Render("→ " + header))
				} else {
					if item.DC == "Testers" {
						content.WriteString(testerHeaderStyle.Render("  " + header))
					} else {
						content.WriteString(dcHeaderStyle.Render("  " + header))
					}
				}
				content.WriteString("\n")

			} else if item.Type == "machine" {
				// Check if machine or DC is selected
				isSelected := false
				for _, m2 := range m.filterMachineList {
					if m2 == item.Machine {
						isSelected = true
						break
					}
				}
				dc := extractDCFromAddress(item.Machine)
				if !isSelected && dc != "" && m.filterMachineDCs[dc] {
					isSelected = true
				}

				checkbox := "[ ]"
				if isSelected {
					checkbox = "[x]"
				}

				machineLine := fmt.Sprintf("    %s %s", checkbox, item.Machine)

				// Check if this machine is selected (highlighted)
				if itemIdx == m.filterMachineSelected {
					// Apply checkbox styling first
					if isSelected {
						machineLine = strings.Replace(machineLine, "[x]", checkedStyle.Render("[x]"), 1)
					}
					content.WriteString(selectedStyle.Render("→ " + strings.TrimPrefix(machineLine, "  ")))
				} else {
					// Apply checkbox styling
					if isSelected {
						machineLine = strings.Replace(machineLine, "[x]", checkedStyle.Render("[x]"), 1)
					}
					content.WriteString(normalStyle.Render(machineLine))
				}
				content.WriteString("\n")

				// Add role lines
				if item.Worker != nil {
					for _, role := range item.Worker.Roles {
						roleLabel := formatRoleLabel(role)
						content.WriteString(roleStyle.Render(fmt.Sprintf("        %s", roleLabel)))
						content.WriteString("\n")
					}
				}
			}
		}
	}

	// Help text
	content.WriteString("\n")
	content.WriteString(normalStyle.Render("Ctrl+N/P: row | Ctrl+F/B: column | Space: toggle | Enter: done | Esc: cancel"))

	popup := popupStyle.Render(content.String())

	// Center the popup
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, popup, lipgloss.WithWhitespaceChars(" "))
}

// renderTypeSearchPopup renders the Type search popup overlay
func (m model) renderTypeSearchPopup(baseView string) string {
	popupStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(1, 2).
		Width(70).
		MaxHeight(m.height - 4)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39")).
		Underline(true)

	normalStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252"))

	selectedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("226")).
		Bold(true)

	var content strings.Builder
	content.WriteString(titleStyle.Render("Search Event Types"))
	content.WriteString("\n\n")

	// Search input
	content.WriteString(normalStyle.Render("Search: "))
	content.WriteString(m.filterTypeSearchInput.View())
	content.WriteString("\n\n")

	// Filter Type list based on search input
	searchTerm := strings.ToLower(m.filterTypeSearchInput.Value())
	var filteredTypes []string
	for _, t := range m.filterTypeSearchList {
		if searchTerm == "" || strings.Contains(strings.ToLower(t), searchTerm) {
			filteredTypes = append(filteredTypes, t)
		}
	}

	// Calculate available height for Type list
	maxDisplayLines := m.height - 14 // Account for title, search, help, padding, border

	// Display filtered Types with scrolling
	if len(filteredTypes) == 0 {
		content.WriteString(normalStyle.Render("  (no matches)"))
		content.WriteString("\n")
	} else {
		// Clamp selected index
		if m.filterTypeSearchSelected >= len(filteredTypes) {
			m.filterTypeSearchSelected = len(filteredTypes) - 1
		}
		if m.filterTypeSearchSelected < 0 {
			m.filterTypeSearchSelected = 0
		}

		// Calculate scroll offset to keep selected item visible
		scrollOffset := 0
		if m.filterTypeSearchSelected >= maxDisplayLines {
			scrollOffset = m.filterTypeSearchSelected - maxDisplayLines + 1
		}

		// Show count if many types
		if len(filteredTypes) > maxDisplayLines {
			countInfo := fmt.Sprintf("  Showing %d-%d of %d types", scrollOffset+1, min(scrollOffset+maxDisplayLines, len(filteredTypes)), len(filteredTypes))
			content.WriteString(normalStyle.Render(countInfo))
			content.WriteString("\n\n")
		}

		// Display visible range
		endIdx := scrollOffset + maxDisplayLines
		if endIdx > len(filteredTypes) {
			endIdx = len(filteredTypes)
		}

		for i := scrollOffset; i < endIdx; i++ {
			typeValue := filteredTypes[i]
			prefix := "  "
			style := normalStyle

			if i == m.filterTypeSearchSelected {
				prefix = "→ "
				style = selectedStyle
			}

			content.WriteString(style.Render(fmt.Sprintf("%s%s", prefix, typeValue)))
			content.WriteString("\n")
		}
	}

	// Help text
	content.WriteString("\n")
	content.WriteString(normalStyle.Render("Ctrl+N/P: navigate | Enter: add filter | Esc: cancel"))

	popup := popupStyle.Render(content.String())

	// Center the popup
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, popup, lipgloss.WithWhitespaceChars(" "))
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// NetworkMetric represents a single network latency measurement
type NetworkMetric struct {
	Time          string
	TimeValue     float64
	Src           string
	Dst           string
	MinLatency    float64
	MaxLatency    float64
	MedianLatency float64
	P90Latency    float64
	TimeoutCount  int
}

// DegradedPeerMetric represents a degraded peer detection event
type DegradedPeerMetric struct {
	Time                      string
	TimeValue                 float64
	Src                       string
	Dst                       string
	Disconnected              string
	MinLatency                float64
	MaxLatency                float64
	MedianLatency             float64
	CheckedPercentileLatency  float64
	ConnectionFailureCount    int
}

// ConnectionMetric represents a Sim2Connection or SimulatedDisconnection event
type ConnectionMetric struct {
	Time          string
	TimeValue     float64
	Src           string
	Dst           string
	Latency       float64
	Disconnection string // Phase value from SimulatedDisconnection events
}

// collectNetworkMetrics collects PingLatency events up to current time
// Returns the latest metric for each (src, dst) pair
func (m *model) collectNetworkMetrics() []NetworkMetric {
	// Map to store latest metric for each (src, dst) pair
	metricsMap := make(map[string]*NetworkMetric)

	// Scan all events up to current event index
	for i := 0; i <= m.currentEventIndex && i < len(m.traceData.Events); i++ {
		event := &m.traceData.Events[i]

		// Filter for PingLatency events
		if event.Type != "PingLatency" {
			continue
		}

		// Skip if no Machine or PeerAddress
		src := event.Machine
		dst := event.Attrs["PeerAddress"]
		if dst == "" {
			dst = event.Attrs["PeerAddr"]
		}
		if src == "" || dst == "" {
			continue
		}

		// Skip 0.0.0.0 addresses (invalid/placeholder)
		if strings.HasPrefix(src, "0.0.0.0") || strings.HasPrefix(dst, "0.0.0.0") {
			continue
		}

		// Create key for (src, dst) pair
		key := src + "|" + dst

		// Parse metrics from event attributes
		metric := NetworkMetric{
			Time:      event.Time,
			TimeValue: event.TimeValue,
			Src:       src,
			Dst:       dst,
		}

		// Parse latencies - skip this metric if any value is invalid or unreasonably large
		validMetric := true
		if val, err := strconv.ParseFloat(event.Attrs["MinLatency"], 64); err == nil && val < 1000.0 {
			metric.MinLatency = val
		} else {
			validMetric = false
		}
		if val, err := strconv.ParseFloat(event.Attrs["MaxLatency"], 64); err == nil && val < 1000.0 {
			metric.MaxLatency = val
		} else {
			validMetric = false
		}
		if val, err := strconv.ParseFloat(event.Attrs["MedianLatency"], 64); err == nil && val < 1000.0 {
			metric.MedianLatency = val
		} else {
			validMetric = false
		}
		if val, err := strconv.ParseFloat(event.Attrs["P90Latency"], 64); err == nil && val < 1000.0 {
			metric.P90Latency = val
		} else {
			validMetric = false
		}
		if val, err := strconv.Atoi(event.Attrs["TimeoutCount"]); err == nil {
			metric.TimeoutCount = val
		}

		// Only keep valid metrics
		if !validMetric {
			continue
		}

		// Keep latest event for this (src, dst) pair
		if existing, found := metricsMap[key]; !found || event.TimeValue > existing.TimeValue {
			metricsMap[key] = &metric
		}
	}

	// Convert map to slice
	var metrics []NetworkMetric
	for _, metric := range metricsMap {
		metrics = append(metrics, *metric)
	}

	// Sort by (MedianLatency desc, Src, Dst) for deterministic ordering
	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].MedianLatency != metrics[j].MedianLatency {
			return metrics[i].MedianLatency > metrics[j].MedianLatency
		}
		if metrics[i].Src != metrics[j].Src {
			return metrics[i].Src < metrics[j].Src
		}
		return metrics[i].Dst < metrics[j].Dst
	})

	return metrics
}

// collectDegradedPeerMetrics collects HealthMonitorDetectDegradedPeer events up to current time
// Returns the latest metric for each (src, dst) pair, sorted by dst
func (m *model) collectDegradedPeerMetrics() []DegradedPeerMetric {
	// Map to store latest metric for each (src, dst) pair
	metricsMap := make(map[string]*DegradedPeerMetric)

	// Scan all events up to current event index
	for i := 0; i <= m.currentEventIndex && i < len(m.traceData.Events); i++ {
		event := &m.traceData.Events[i]

		// Filter for HealthMonitorDetectDegradedPeer events
		if event.Type != "HealthMonitorDetectDegradedPeer" {
			continue
		}

		// Skip if no Machine or PeerAddress
		src := event.Machine
		dst := event.Attrs["PeerAddress"]
		if dst == "" {
			continue
		}
		if src == "" {
			continue
		}

		// Skip 0.0.0.0 addresses (invalid/placeholder)
		if strings.HasPrefix(src, "0.0.0.0") || strings.HasPrefix(dst, "0.0.0.0") {
			continue
		}

		// Create key for (src, dst) pair
		key := src + "|" + dst

		// Parse metric from event attributes
		metric := DegradedPeerMetric{
			Time:         event.Time,
			TimeValue:    event.TimeValue,
			Src:          src,
			Dst:          dst,
			Disconnected: event.Attrs["Disconnected"],
		}

		// Parse latencies - skip invalid or unreasonably large values
		if val, err := strconv.ParseFloat(event.Attrs["MinLatency"], 64); err == nil && val < 1000.0 {
			metric.MinLatency = val
		}
		if val, err := strconv.ParseFloat(event.Attrs["MaxLatency"], 64); err == nil && val < 1000.0 {
			metric.MaxLatency = val
		}
		if val, err := strconv.ParseFloat(event.Attrs["MedianLatency"], 64); err == nil && val < 1000.0 {
			metric.MedianLatency = val
		}
		if val, err := strconv.ParseFloat(event.Attrs["CheckedPercentileLatency"], 64); err == nil && val < 1000.0 {
			metric.CheckedPercentileLatency = val
		}
		if val, err := strconv.Atoi(event.Attrs["ConnectionFailureCount"]); err == nil {
			metric.ConnectionFailureCount = val
		}

		// Keep latest event for this (src, dst) pair
		if existing, found := metricsMap[key]; !found || event.TimeValue > existing.TimeValue {
			metricsMap[key] = &metric
		}
	}

	// Convert map to slice
	var metrics []DegradedPeerMetric
	for _, metric := range metricsMap {
		metrics = append(metrics, *metric)
	}

	// Sort by (MedianLatency desc, Src, Dst) for deterministic ordering
	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].MedianLatency != metrics[j].MedianLatency {
			return metrics[i].MedianLatency > metrics[j].MedianLatency
		}
		if metrics[i].Src != metrics[j].Src {
			return metrics[i].Src < metrics[j].Src
		}
		return metrics[i].Dst < metrics[j].Dst
	})

	return metrics
}

// collectConnectionMetrics collects Sim2Connection and SimulatedDisconnection events up to current time
// Returns the latest metric for each (src, dst) pair, sorted by latency (descending)
func (m *model) collectConnectionMetrics() []ConnectionMetric {
	// Map to store latest metric for each (src, dst) pair
	metricsMap := make(map[string]*ConnectionMetric)

	// Scan all events up to current event index
	for i := 0; i <= m.currentEventIndex && i < len(m.traceData.Events); i++ {
		event := &m.traceData.Events[i]

		var src, dst string
		var metric ConnectionMetric

		if event.Type == "Sim2Connection" {
			// Handle Sim2Connection events
			src = event.Attrs["From"]
			dst = event.Attrs["To"]
			if src == "" || dst == "" {
				continue
			}

			// Skip 0.0.0.0 addresses (invalid/placeholder)
			if strings.HasPrefix(src, "0.0.0.0") || strings.HasPrefix(dst, "0.0.0.0") {
				continue
			}

			metric = ConnectionMetric{
				Time:      event.Time,
				TimeValue: event.TimeValue,
				Src:       src,
				Dst:       dst,
			}

			// Parse latency - skip invalid or unreasonably large values
			if val, err := strconv.ParseFloat(event.Attrs["Latency"], 64); err == nil && val < 1000.0 {
				metric.Latency = val
			}

		} else if event.Type == "SimulatedDisconnection" {
			// Handle SimulatedDisconnection events
			src = event.Attrs["Address"]
			dst = event.Attrs["PeerAddress"]
			if src == "" || dst == "" {
				continue
			}

			// Skip 0.0.0.0 addresses (invalid/placeholder)
			if strings.HasPrefix(src, "0.0.0.0") || strings.HasPrefix(dst, "0.0.0.0") {
				continue
			}

			metric = ConnectionMetric{
				Time:          event.Time,
				TimeValue:     event.TimeValue,
				Src:           src,
				Dst:           dst,
				Disconnection: event.Attrs["Phase"],
			}

		} else {
			continue
		}

		// Create key for (src, dst) pair
		key := src + "|" + dst

		// Keep latest event for this (src, dst) pair
		if existing, found := metricsMap[key]; !found || event.TimeValue > existing.TimeValue {
			metricsMap[key] = &metric
		}
	}

	// Convert map to slice
	var metrics []ConnectionMetric
	for _, metric := range metricsMap {
		metrics = append(metrics, *metric)
	}

	// Sort by (Latency desc, Src, Dst) for deterministic ordering
	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].Latency != metrics[j].Latency {
			return metrics[i].Latency > metrics[j].Latency
		}
		if metrics[i].Src != metrics[j].Src {
			return metrics[i].Src < metrics[j].Src
		}
		return metrics[i].Dst < metrics[j].Dst
	})

	return metrics
}

// renderHealthPopup renders the health metrics popup overlay
func (m model) renderHealthPopup(baseView string) string {
	popupStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(1, 2).
		MaxWidth(m.width - 4).
		MaxHeight(m.height - 4)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39")).
		Underline(true)

	sectionStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("46")).
		MarginTop(1)

	headerStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("33")).
		Bold(true)

	normalStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252"))

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241")).
		MarginTop(1)

	scrollIndicatorStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240")).
		Italic(true)

	var content strings.Builder
	content.WriteString(titleStyle.Render(fmt.Sprintf("Cluster Health Snapshot (t=%.6fs)", m.currentTime)))
	content.WriteString("\n\n")

	// Network section
	content.WriteString(sectionStyle.Render("NETWORK LATENCIES (PingLatency)"))
	content.WriteString("\n\n")

	// Collect network metrics
	metrics := m.collectNetworkMetrics()

	if len(metrics) == 0 {
		content.WriteString(normalStyle.Render("  No PingLatency events found"))
		content.WriteString("\n")
	} else {
		// Table header
		header := fmt.Sprintf("%-12s  %-21s  %-21s  %8s  %8s  %8s  %8s  %8s",
			"Time", "Src", "Dst", "MinLat", "MaxLat", "MedLat", "P90Lat", "Timeouts")
		content.WriteString(headerStyle.Render(header))
		content.WriteString("\n")

		// Separator line
		separator := strings.Repeat("─", 130)
		content.WriteString(normalStyle.Render(separator))
		content.WriteString("\n")

		// Table rows (show top 5 only)
		displayCount := len(metrics)
		if displayCount > 5 {
			displayCount = 5
		}

		for i := 0; i < displayCount; i++ {
			metric := metrics[i]
			row := fmt.Sprintf("%-12s  %-21s  %-21s  %7.3fs  %7.3fs  %7.3fs  %7.3fs  %8d",
				metric.Time,
				truncateAddr(metric.Src, 21),
				truncateAddr(metric.Dst, 21),
				metric.MinLatency,
				metric.MaxLatency,
				metric.MedianLatency,
				metric.P90Latency,
				metric.TimeoutCount)
			content.WriteString(normalStyle.Render(row))
			content.WriteString("\n")
		}

		if len(metrics) > 5 {
			content.WriteString(normalStyle.Render(fmt.Sprintf("  ... %d more entries not shown", len(metrics)-5)))
			content.WriteString("\n")
		}
	}

	// Degraded Peers section
	content.WriteString("\n")
	content.WriteString(sectionStyle.Render("DEGRADED PEERS (HealthMonitorDetectDegradedPeer)"))
	content.WriteString("\n\n")

	// Collect degraded peer metrics
	degradedMetrics := m.collectDegradedPeerMetrics()

	if len(degradedMetrics) == 0 {
		content.WriteString(normalStyle.Render("  No HealthMonitorDetectDegradedPeer events found"))
		content.WriteString("\n")
	} else {
		// Table header
		header := fmt.Sprintf("%-12s  %-21s  %-21s  %12s  %8s  %8s  %8s  %10s  %10s",
			"Time", "Src", "Dst (disc/deg)", "Disconnected", "MinLat", "MaxLat", "MedLat", "P%Lat", "ConnFail")
		content.WriteString(headerStyle.Render(header))
		content.WriteString("\n")

		// Separator line
		separator := strings.Repeat("─", 130)
		content.WriteString(normalStyle.Render(separator))
		content.WriteString("\n")

		// Table rows (show top 5 only)
		displayCount := len(degradedMetrics)
		if displayCount > 5 {
			displayCount = 5
		}

		for i := 0; i < displayCount; i++ {
			metric := degradedMetrics[i]
			row := fmt.Sprintf("%-12s  %-21s  %-21s  %12s  %7.3fs  %7.3fs  %7.3fs  %9.3fs  %10d",
				metric.Time,
				truncateAddr(metric.Src, 21),
				truncateAddr(metric.Dst, 21),
				metric.Disconnected,
				metric.MinLatency,
				metric.MaxLatency,
				metric.MedianLatency,
				metric.CheckedPercentileLatency,
				metric.ConnectionFailureCount)
			content.WriteString(normalStyle.Render(row))
			content.WriteString("\n")
		}

		if len(degradedMetrics) > 5 {
			content.WriteString(normalStyle.Render(fmt.Sprintf("  ... %d more entries not shown", len(degradedMetrics)-5)))
			content.WriteString("\n")
		}
	}

	// Sim2Connection and SimulatedDisconnection section
	content.WriteString("\n")
	content.WriteString(sectionStyle.Render("CONNECTIONS (Sim2Connection / SimulatedDisconnection)"))
	content.WriteString("\n\n")

	// Collect connection metrics
	connMetrics := m.collectConnectionMetrics()

	if len(connMetrics) == 0 {
		content.WriteString(normalStyle.Render("  No Sim2Connection or SimulatedDisconnection events found"))
		content.WriteString("\n")
	} else {
		// Table header
		header := fmt.Sprintf("%-12s  %-21s  %-21s  %10s  %14s",
			"Time", "Src", "Dst", "Latency", "Disconnection")
		content.WriteString(headerStyle.Render(header))
		content.WriteString("\n")

		// Separator line
		separator := strings.Repeat("─", 85)
		content.WriteString(normalStyle.Render(separator))
		content.WriteString("\n")

		// Table rows (show top 5 only)
		displayCount := len(connMetrics)
		if displayCount > 5 {
			displayCount = 5
		}

		for i := 0; i < displayCount; i++ {
			metric := connMetrics[i]
			row := fmt.Sprintf("%-12s  %-21s  %-21s  %9.3fs  %14s",
				metric.Time,
				truncateAddr(metric.Src, 21),
				truncateAddr(metric.Dst, 21),
				metric.Latency,
				metric.Disconnection)
			content.WriteString(normalStyle.Render(row))
			content.WriteString("\n")
		}

		if len(connMetrics) > 5 {
			content.WriteString(normalStyle.Render(fmt.Sprintf("  ... %d more entries not shown", len(connMetrics)-5)))
			content.WriteString("\n")
		}
	}

	// Split content into lines for scrolling
	contentLines := strings.Split(content.String(), "\n")
	totalLines := len(contentLines)

	// Calculate available height for content
	// Account for: title (1 line) + top margin (1) + help text (2) + bottom margin (1) + padding (2) + border (2) = 9 lines
	maxContentHeight := m.height - 9
	if maxContentHeight < 5 {
		maxContentHeight = 5 // Minimum visible lines
	}

	// Clamp scroll offset
	maxScrollOffset := totalLines - maxContentHeight
	if maxScrollOffset < 0 {
		maxScrollOffset = 0
	}

	displayScrollOffset := m.healthScrollOffset
	if displayScrollOffset < 0 {
		displayScrollOffset = 0
	}
	if displayScrollOffset > maxScrollOffset {
		displayScrollOffset = maxScrollOffset
	}

	// Determine if we have more content above/below
	hasMoreAbove := displayScrollOffset > 0
	hasMoreBelow := displayScrollOffset < maxScrollOffset

	// Calculate visible window
	visibleLines := contentLines
	if totalLines > maxContentHeight {
		endIdx := displayScrollOffset + maxContentHeight
		if endIdx > totalLines {
			endIdx = totalLines
		}
		visibleLines = contentLines[displayScrollOffset:endIdx]
	}

	// Build content with scroll indicators
	var scrollableContent strings.Builder

	if hasMoreAbove {
		scrollableContent.WriteString(scrollIndicatorStyle.Render("↑ more above"))
		scrollableContent.WriteString("\n")
	}

	scrollableContent.WriteString(strings.Join(visibleLines, "\n"))

	if hasMoreBelow {
		scrollableContent.WriteString("\n")
		scrollableContent.WriteString(scrollIndicatorStyle.Render("↓ more below"))
	}

	scrollableContent.WriteString("\n")
	scrollableContent.WriteString(helpStyle.Render("\nPress q/x/Esc to close | Ctrl+N/P to scroll"))

	popup := popupStyle.Render(scrollableContent.String())

	// Center the popup
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, popup, lipgloss.WithWhitespaceChars(" "))
}

// truncateAddr truncates an address to fit within maxLen characters
func truncateAddr(addr string, maxLen int) string {
	if len(addr) <= maxLen {
		return addr
	}
	// Truncate from the middle to keep both start and end visible
	halfLen := (maxLen - 3) / 2
	return addr[:halfLen] + "..." + addr[len(addr)-halfLen:]
}

// renderHelpPopup renders the help information popup overlay
func (m model) renderHelpPopup(baseView string) string {
	popupStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(1, 2).
		Width(80)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39")).
		Underline(true)

	sectionStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("46")).
		MarginTop(1)

	commandStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252"))

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241")).
		MarginTop(1)

	var content strings.Builder

	// ASCII Art Header - Time Wizard
	asciiArt := `
    ⏪ ═══════════════════════ ⏩
                  ★
                 /|
                / |
               /  |
              /  ✨|
             /    |
            /_____|
            ( o  o )
             \  > /
          ~~~~~~~~~~~
          \  ~~~~  /
           \      /
        ✨  |    |  ✨
      ◀──── |    | ────▶
           /      \
          ⌛  🔮  ⌛
    ═══════════════════════
          FDB REPLAY
       Time Travel Wizard`

	content.WriteString(titleStyle.Render(asciiArt))
	content.WriteString("\n\n")

	// Navigation section
	content.WriteString(sectionStyle.Render("Navigation:"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  Ctrl+N / Ctrl+P    Next / previous trace event"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  Ctrl+V / Alt+V     Page forward / backward (±1 second)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  g / G              Jump to start / end"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  t                  Jump to specific time"))
	content.WriteString("\n\n")

	// Search section
	content.WriteString(sectionStyle.Render("Search:"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  /                  Search forward (use * for wildcard)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  ?                  Search backward (use * for wildcard)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  n                  Go to next match"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  N                  Go to previous match"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  Esc                Clear search highlighting"))
	content.WriteString("\n\n")

	// Filter section
	content.WriteString(sectionStyle.Render("Filter:"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  f                  Open filter configuration popup"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  Space              Toggle 'All' to enable/disable filtering"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  1/2/3              Jump to Raw/Machine/Time category"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  Ctrl+N/P           Switch categories (or navigate within Raw filters)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  a                  Add new raw filter (wildcard patterns: Type=*Recovery*)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  t                  Search and add Type filter (fuzzy match Type values)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  e                  Edit selected raw filter"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  r                  Remove selected raw filter"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  d                  Toggle disable on selected raw filter (Raw category)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("                     Toggle time filter on/off (Time category)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  c                  Toggle common trace event filters (Raw category)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("                     Adds/removes pre-defined important event types"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  Ctrl+F/B           Navigate columns (in Raw filters)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  Enter              Configure Machine/Time filters"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("                     Filters use AND logic between categories, OR within"))
	content.WriteString("\n\n")

	// Recovery section
	content.WriteString(sectionStyle.Render("Recovery Navigation:"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  r / R              Jump to next / prev recovery start (StatusCode=0)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  e / E              Jump to next / prev MasterRecoveryState (any)"))
	content.WriteString("\n\n")

	// Severity Navigation section
	content.WriteString(sectionStyle.Render("Severity Navigation:"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  3 / Shift+3        Jump to next / prev Severity=30 event"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  4 / Shift+4        Jump to next / prev Severity=40 event"))
	content.WriteString("\n\n")

	// View section
	content.WriteString(sectionStyle.Render("Views:"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  c                  Show full DB config JSON (Ctrl+N/P to scroll)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  x                  Show health metrics (network, degraded peers, connections)"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  h                  Show this help"))
	content.WriteString("\n\n")

	// General section
	content.WriteString(sectionStyle.Render("General:"))
	content.WriteString("\n")
	content.WriteString(commandStyle.Render("  q / Q / Ctrl+C     Quit"))
	content.WriteString("\n")

	content.WriteString(helpStyle.Render("\nPress q/h/Esc to close"))

	popup := popupStyle.Render(content.String())

	// Overlay the popup on top of the base view
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, popup, lipgloss.WithWhitespaceChars(" "))
}

// renderNoConfigPopup renders a message when no config is available
func (m model) renderNoConfigPopup(baseView string) string {
	popupStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(1, 2).
		Width(50)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39"))

	messageStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("243")).
		MarginTop(1)

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241")).
		MarginTop(1)

	popupContent := titleStyle.Render("DB Config") + "\n" +
		messageStyle.Render("No configuration available yet at this time.") + "\n" +
		helpStyle.Render("Press q/c/Esc to close")

	popup := popupStyle.Render(popupContent)

	// Overlay the popup on top of the base view
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, popup, lipgloss.WithWhitespaceChars(" "))
}

// renderConfigPopup renders the full config JSON popup overlay
func (m model) renderConfigPopup(baseView string, config *DBConfig) string {
	popupStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(1, 2).
		MaxWidth(m.width - 10).
		MaxHeight(m.height - 4)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39"))

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241")).
		MarginTop(1)

	jsonStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("252"))

	scrollIndicatorStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240")).
		Italic(true)

	// Pretty-print the JSON
	jsonBytes, err := json.MarshalIndent(config.RawJSON, "", "  ")
	var jsonContent string
	if err != nil {
		jsonContent = "Error formatting JSON"
	} else {
		jsonContent = string(jsonBytes)
	}

	// Split JSON into lines
	jsonLines := strings.Split(jsonContent, "\n")
	totalLines := len(jsonLines)

	// Calculate available height for JSON content
	// Account for: title (1 line) + top margin (1) + help text (1) + bottom margin (1) + padding (2) + border (2) = 8 lines
	maxContentHeight := m.height - 12
	if maxContentHeight < 5 {
		maxContentHeight = 5 // Minimum visible lines
	}

	// Clamp scroll offset
	maxScrollOffset := totalLines - maxContentHeight
	if maxScrollOffset < 0 {
		maxScrollOffset = 0
	}

	displayScrollOffset := m.configScrollOffset
	if displayScrollOffset < 0 {
		displayScrollOffset = 0
	}
	if displayScrollOffset > maxScrollOffset {
		displayScrollOffset = maxScrollOffset
	}

	// Determine if we have more content above/below
	hasMoreAbove := displayScrollOffset > 0
	hasMoreBelow := displayScrollOffset < maxScrollOffset

	// Calculate visible window
	visibleLines := jsonLines
	if totalLines > maxContentHeight {
		endIdx := displayScrollOffset + maxContentHeight
		if endIdx > totalLines {
			endIdx = totalLines
		}
		visibleLines = jsonLines[displayScrollOffset:endIdx]
	}

	// Build JSON content with scroll indicators
	var jsonContentBuilder strings.Builder

	if hasMoreAbove {
		jsonContentBuilder.WriteString(scrollIndicatorStyle.Render("↑ more above"))
		jsonContentBuilder.WriteString("\n")
	}

	jsonContentBuilder.WriteString(jsonStyle.Render(strings.Join(visibleLines, "\n")))

	if hasMoreBelow {
		jsonContentBuilder.WriteString("\n")
		jsonContentBuilder.WriteString(scrollIndicatorStyle.Render("↓ more below"))
	}

	// Build popup content
	popupContent := titleStyle.Render(fmt.Sprintf("DB Config (t=%.2fs)", config.Time)) + "\n\n" +
		jsonContentBuilder.String() + "\n\n" +
		helpStyle.Render("Press q/c/Esc to close | Ctrl+N/P to scroll")

	popup := popupStyle.Render(popupContent)

	// Overlay the popup on top of the base view
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, popup, lipgloss.WithWhitespaceChars(" "))
}

// renderTimeInputPopup renders the time input popup overlay
func (m model) renderTimeInputPopup(baseView string) string {
	popupStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("39")).
		Padding(1, 2).
		Width(50)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("39"))

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241")).
		MarginTop(1)

	errorStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("196")).
		MarginTop(1)

	rangeStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("243")).
		Italic(true)

	// Validate the current input
	var validationMsg string
	if inputValue := m.timeInput.Value(); inputValue != "" {
		if targetTime, err := strconv.ParseFloat(inputValue, 64); err != nil {
			validationMsg = errorStyle.Render("✗ Invalid number format")
		} else if targetTime < m.traceData.MinTime {
			validationMsg = errorStyle.Render(fmt.Sprintf("✗ Time must be >= %.2f", m.traceData.MinTime))
		} else if targetTime > m.traceData.MaxTime {
			validationMsg = errorStyle.Render(fmt.Sprintf("✗ Time must be <= %.2f", m.traceData.MaxTime))
		} else {
			validationMsg = lipgloss.NewStyle().Foreground(lipgloss.Color("46")).Render("✓ Valid")
		}
	}

	rangeInfo := rangeStyle.Render(fmt.Sprintf("Valid range: %.2f - %.2f seconds", m.traceData.MinTime, m.traceData.MaxTime))

	popupContent := titleStyle.Render("Jump to Time") + "\n\n" +
		m.timeInput.View() + "\n"

	if validationMsg != "" {
		popupContent += validationMsg + "\n"
	}

	popupContent += "\n" + rangeInfo + "\n" +
		helpStyle.Render("Enter: jump | Esc/q/t: cancel")

	popup := popupStyle.Render(popupContent)

	// Overlay the popup on top of the base view
	// Place it roughly in the center
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, popup, lipgloss.WithWhitespaceChars(" "))
}

// convertWildcardToRegex converts a simple wildcard pattern to regex
// * matches 0 or more characters
func convertWildcardToRegex(pattern string) string {
	// Escape regex special characters except *
	var result strings.Builder
	for _, ch := range pattern {
		switch ch {
		case '*':
			result.WriteString(".*")
		case '.', '+', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\':
			result.WriteRune('\\')
			result.WriteRune(ch)
		default:
			result.WriteRune(ch)
		}
	}
	return result.String()
}

// extractLiterals extracts the non-wildcard literal parts from a search pattern
// For example: "*Recovery*State*" -> ["Recovery", "State"]
func extractLiterals(pattern string) []string {
	// Split by * to get literal parts
	parts := strings.Split(pattern, "*")
	var literals []string
	for _, part := range parts {
		if part != "" {
			literals = append(literals, part)
		}
	}
	return literals
}

// getEventFullText builds a full text representation of an event including ALL fields
func getEventFullText(event *TraceEvent) string {
	var parts []string

	// Include all standard fields
	if event.Time != "" {
		parts = append(parts, "Time="+event.Time)
	}
	if event.Type != "" {
		parts = append(parts, "Type="+event.Type)
	}
	if event.Severity != "" {
		parts = append(parts, "Severity="+event.Severity)
	}
	if event.Machine != "" {
		parts = append(parts, "Machine="+event.Machine)
	}
	if event.ID != "" {
		parts = append(parts, "ID="+event.ID)
	}

	// Include all attributes (sorted for consistency)
	var attrKeys []string
	for key := range event.Attrs {
		attrKeys = append(attrKeys, key)
	}
	sort.Strings(attrKeys)

	for _, key := range attrKeys {
		value := event.Attrs[key]
		parts = append(parts, key+"="+value)
	}

	return strings.Join(parts, " ")
}

// searchForward searches for pattern starting from startIndex going forward
// Returns the index of the first matching event, or -1 if not found
// Respects active filters - only searches visible (non-filtered) events
func (m *model) searchForward(startIndex int, pattern string) int {
	regexPattern := convertWildcardToRegex(pattern)
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return -1
	}

	for i := startIndex; i < len(m.traceData.Events); i++ {
		event := &m.traceData.Events[i]

		// Skip filtered events
		if !eventMatchesFilters(event, m) {
			continue
		}

		eventText := getEventFullText(event)
		if re.MatchString(eventText) {
			return i
		}
	}

	// Wrap around to beginning
	for i := 0; i < startIndex; i++ {
		event := &m.traceData.Events[i]

		// Skip filtered events
		if !eventMatchesFilters(event, m) {
			continue
		}

		eventText := getEventFullText(event)
		if re.MatchString(eventText) {
			return i
		}
	}

	return -1
}

// searchBackward searches for pattern starting from startIndex going backward
// Returns the index of the first matching event, or -1 if not found
// Respects active filters - only searches visible (non-filtered) events
func (m *model) searchBackward(startIndex int, pattern string) int {
	regexPattern := convertWildcardToRegex(pattern)
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return -1
	}

	for i := startIndex; i >= 0; i-- {
		event := &m.traceData.Events[i]

		// Skip filtered events
		if !eventMatchesFilters(event, m) {
			continue
		}

		eventText := getEventFullText(event)
		if re.MatchString(eventText) {
			return i
		}
	}

	// Wrap around to end
	for i := len(m.traceData.Events) - 1; i > startIndex; i-- {
		event := &m.traceData.Events[i]

		// Skip filtered events
		if !eventMatchesFilters(event, m) {
			continue
		}

		eventText := getEventFullText(event)
		if re.MatchString(eventText) {
			return i
		}
	}

	return -1
}

// recompileRawFilterRegexes pre-compiles all raw filter regex patterns for performance
// This should be called whenever filterRawList changes (add, remove, edit, toggle common)
func (m *model) recompileRawFilterRegexes() {
	m.filterRawCompiledRegex = make([]*regexp.Regexp, len(m.filterRawList))
	for i, filter := range m.filterRawList {
		regexPattern := convertWildcardToRegex(filter)
		re, err := regexp.Compile(regexPattern)
		if err != nil {
			// Store nil for invalid patterns
			m.filterRawCompiledRegex[i] = nil
		} else {
			m.filterRawCompiledRegex[i] = re
		}
	}
}

// rebuildMachineSet rebuilds the machine set for O(1) lookups
// This should be called whenever filterMachineList changes
func (m *model) rebuildMachineSet() {
	m.filterMachineSet = make(map[string]bool)
	for _, machine := range m.filterMachineList {
		m.filterMachineSet[machine] = true
	}
}

// getCachedDC returns the DC for a machine address, using cache for performance
func (m *model) getCachedDC(machineAddr string) string {
	// Check cache first
	if dc, found := m.machineDCCache[machineAddr]; found {
		return dc
	}

	// Extract and cache
	dc := extractDCFromAddress(machineAddr)
	m.machineDCCache[machineAddr] = dc
	return dc
}

// eventMatchesFilters checks if an event matches any of the filter patterns (OR logic)
// Returns true if:
// - showAll is true, OR
// - filterList is empty, OR
// - event matches at least one filter pattern
func eventMatchesFilters(event *TraceEvent, m *model) bool {
	// If "All" is checked, show everything
	if m.filterShowAll {
		return true
	}

	// If "All" is unchecked but no filters are set, show nothing
	if len(m.filterRawList) == 0 && len(m.filterMachineList) == 0 && len(m.filterMachineDCs) == 0 && !m.filterTimeEnabled && !m.filterMessageEnabled {
		return false
	}

	// Apply filters with AND precedence: Time AND Machine AND Raw AND Message

	// 1. Time filter (highest precedence)
	if m.filterTimeEnabled {
		if event.TimeValue < m.filterTimeStart || event.TimeValue > m.filterTimeEnd {
			return false
		}
	}

	// 2. Machine filter (OR within category)
	if len(m.filterMachineList) > 0 || len(m.filterMachineDCs) > 0 {
		machineMatches := false

		// Check if machine is in selected list using O(1) set lookup
		if m.filterMachineSet[event.Machine] {
			machineMatches = true
		}

		// Check if machine's DC is selected using cached DC extraction
		if !machineMatches {
			dc := m.getCachedDC(event.Machine)
			if dc != "" && m.filterMachineDCs[dc] {
				machineMatches = true
			}
		}

		if !machineMatches {
			return false
		}
	}

	// 3. Raw filters (OR within category) - skip disabled filters
	if len(m.filterRawList) > 0 {
		eventText := getEventFullText(event)
		rawMatches := false

		for i := range m.filterRawList {
			// Skip disabled filters
			if m.filterRawDisabled[i] {
				continue
			}

			// Use pre-compiled regex for performance
			if i < len(m.filterRawCompiledRegex) && m.filterRawCompiledRegex[i] != nil {
				if m.filterRawCompiledRegex[i].MatchString(eventText) {
					rawMatches = true
					break
				}
			}
		}

		if !rawMatches {
			return false
		}
	}

	// 4. Message filter
	if m.filterMessageEnabled {
		if event.Type != "NetworkMessageSent" {
			return false
		}
	}

	return true
}

// normalizeAddress removes the :tls suffix from machine addresses for comparison
// FDB is inconsistent about including :tls in addresses, so we normalize for matching
// Example: "[abcd::2:0:1:1]:1:tls" -> "[abcd::2:0:1:1]:1"
func normalizeAddress(addr string) string {
	return strings.TrimSuffix(addr, ":tls")
}

// stripRPCName removes wrapper types (ErrorOr, EnsureTable) from RPC names
// Example: "ErrorOr<EnsureTable<GetReadVersionReply>>" -> "GetReadVersionReply"
// Example: "ErrorOr<EnsureTable<Void>>" -> "Void"
func stripRPCName(rpcName string) string {
	result := rpcName

	// Remove ErrorOr< and corresponding >
	for {
		start := strings.Index(result, "ErrorOr<")
		if start == -1 {
			break
		}
		// Find the matching closing >
		depth := 1
		end := start + len("ErrorOr<")
		for end < len(result) && depth > 0 {
			if result[end] == '<' {
				depth++
			} else if result[end] == '>' {
				depth--
			}
			end++
		}
		// Extract the inner content
		if depth == 0 && end <= len(result) {
			result = result[start+len("ErrorOr<") : end-1]
		} else {
			break
		}
	}

	// Remove EnsureTable< and corresponding >
	for {
		start := strings.Index(result, "EnsureTable<")
		if start == -1 {
			break
		}
		// Find the matching closing >
		depth := 1
		end := start + len("EnsureTable<")
		for end < len(result) && depth > 0 {
			if result[end] == '<' {
				depth++
			} else if result[end] == '>' {
				depth--
			}
			end++
		}
		// Extract the inner content
		if depth == 0 && end <= len(result) {
			result = result[start+len("EnsureTable<") : end-1]
		} else {
			break
		}
	}

	// Trim any remaining whitespace
	result = strings.TrimSpace(result)
	return result
}

// runUI starts the Bubbletea TUI program
func runUI(traceData *TraceData) error {
	p := tea.NewProgram(
		newModel(traceData),
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running UI: %w", err)
	}

	return nil
}
