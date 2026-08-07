package main

import (
	"fmt"
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

// applyTheme decides which palette variant to use.
//
// An explicit REPLAY_THEME=light|dark wins. Otherwise, inside tmux the outer
// terminal is asked directly, because termenv will not ask on our behalf there -
// see queryBackgroundThroughMultiplexer. Outside tmux, termenv's own detection
// works and is left alone.
//
// Setting REPLAY_THEME, or answering from the query below, marks the background
// explicit, which also stops lipgloss from querying later.
func applyTheme() {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("REPLAY_THEME"))) {
	case "light":
		lipgloss.SetHasDarkBackground(false)
		return
	case "dark":
		lipgloss.SetHasDarkBackground(true)
		return
	}

	if r, g, b, ok := queryBackgroundThroughMultiplexer(); ok {
		lipgloss.SetHasDarkBackground(backgroundIsDark(r, g, b))
		return
	}

	// Nothing explicit and nobody answered, so the dark palette may be about to
	// be used purely because there was no way to find out. Say so, rather than
	// letting a light terminal look broken for no visible reason.
	if backgroundIsAGuess() {
		fmt.Fprintln(os.Stderr, "replay: cannot detect the terminal background, assuming dark.")
		fmt.Fprintln(os.Stderr, "        If the colors look wrong, set REPLAY_THEME=light (or dark).")
	}
}

// backgroundIsAGuess reports whether the background is about to be assumed
// rather than determined. It mirrors the conditions termenv uses: the OSC query
// is skipped for screen and tmux, and $COLORFGBG is the only other source.
func backgroundIsAGuess() bool {
	term := os.Getenv("TERM")
	if !strings.HasPrefix(term, "screen") && !strings.HasPrefix(term, "tmux") {
		return false
	}
	return os.Getenv("COLORFGBG") == ""
}
