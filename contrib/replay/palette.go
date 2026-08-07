package main

import (
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Every color the UI uses is defined here once, with a variant for light and a
// variant for dark terminal backgrounds. lipgloss resolves an AdaptiveColor at
// render time, picking the variant that matches the detected background, so
// none of this depends on package initialization order.
//
// This used to be a single set of 256-color indices picked against a dark
// background, spread across the UI code. On a light background the brighter
// half of that palette - near-white body text, saturated green and yellow, the
// dark olive highlight - lands somewhere between low contrast and invisible.
//
// The rule for the two variants: on a dark background prominence comes from
// being brighter, on a light background from being darker. The gray ramp below
// is therefore inverted between the two.
var (
	// Body text, most to least prominent.
	colText          = lipgloss.AdaptiveColor{Light: "235", Dark: "252"}
	colTextSecondary = lipgloss.AdaptiveColor{Light: "239", Dark: "243"}
	colTextMuted     = lipgloss.AdaptiveColor{Light: "243", Dark: "241"}
	colTextDim       = lipgloss.AdaptiveColor{Light: "245", Dark: "240"}

	// Accents.
	colAccent   = lipgloss.AdaptiveColor{Light: "26", Dark: "39"}   // titles, popup borders
	colDCHeader = lipgloss.AdaptiveColor{Light: "25", Dark: "33"}   // data center headers
	colTester   = lipgloss.AdaptiveColor{Light: "91", Dark: "135"}  // tester headers
	colCurrent  = lipgloss.AdaptiveColor{Light: "30", Dark: "51"}   // current worker/role
	colOk       = lipgloss.AdaptiveColor{Light: "28", Dark: "46"}   // section headers, valid input
	colSelected = lipgloss.AdaptiveColor{Light: "130", Dark: "226"} // selected entry
	colError    = lipgloss.AdaptiveColor{Light: "160", Dark: "196"} // errors
	colRPC      = lipgloss.AdaptiveColor{Light: "130", Dark: "220"} // RPC label under the topology

	// colHighlightBg backs the current line and search matches. It sets no
	// foreground of its own, so it has to stay readable against the terminal's
	// default text color: a light wash on a light background, a dark one on a
	// dark background.
	colHighlightBg = lipgloss.AdaptiveColor{Light: "229", Dark: "58"}

	// The network-message highlight sets its own foreground, and black on
	// yellow reads on either background, so both variants are the same.
	colNetworkBg = lipgloss.AdaptiveColor{Light: "220", Dark: "220"}
	colOnNetwork = lipgloss.AdaptiveColor{Light: "0", Dark: "0"}
)

// applyTheme honors an explicit REPLAY_THEME=light|dark, overriding detection.
//
// lipgloss works out the background by querying the terminal (OSC 11). That
// query is unreliable through tmux and over ssh, and when it fails lipgloss
// assumes a dark background - which is precisely the case that renders
// unreadably on a light terminal. So detection is the default, but there has to
// be a way to say what the background actually is.
func applyTheme() {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("REPLAY_THEME"))) {
	case "light":
		lipgloss.SetHasDarkBackground(false)
	case "dark":
		lipgloss.SetHasDarkBackground(true)
	}
}
