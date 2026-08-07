package main

import (
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

// These tests drive global lipgloss renderer state (color profile and
// background), so they deliberately do not use t.Parallel().

// render returns the escape sequence lipgloss emits for a foreground color,
// with the profile pinned so the output is deterministic.
func render(c lipgloss.TerminalColor) string {
	lipgloss.SetColorProfile(termenv.ANSI256)
	return lipgloss.NewStyle().Foreground(c).Render("x")
}

func TestPaletteFollowsBackground(t *testing.T) {
	cases := []struct {
		name              string
		c                 lipgloss.TerminalColor
		wantDark, wantLgt string
	}{
		{"colText", colText, "252", "235"},
		{"colTextDim", colTextDim, "240", "245"},
		{"colOk", colOk, "46", "28"},
		{"colSelected", colSelected, "226", "130"},
		{"colHighlightBg", colHighlightBg, "58", "229"},
		{"colRPC", colRPC, "220", "130"},
		{"colAccent", colAccent, "39", "26"},
	}

	for _, tc := range cases {
		lipgloss.SetHasDarkBackground(true)
		gotDark := render(tc.c)
		lipgloss.SetHasDarkBackground(false)
		gotLgt := render(tc.c)

		if !strings.Contains(gotDark, "38;5;"+tc.wantDark) {
			t.Errorf("%s on dark bg: want index %s, got %q", tc.name, tc.wantDark, gotDark)
		}
		if !strings.Contains(gotLgt, "38;5;"+tc.wantLgt) {
			t.Errorf("%s on light bg: want index %s, got %q", tc.name, tc.wantLgt, gotLgt)
		}
		if gotDark == gotLgt && tc.wantDark != tc.wantLgt {
			t.Errorf("%s did not change with background", tc.name)
		}
	}
}

// The two deliberately background-independent colors must NOT change.
func TestNetworkHighlightIsBackgroundIndependent(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    lipgloss.TerminalColor
	}{{"colNetworkBg", colNetworkBg}, {"colOnNetwork", colOnNetwork}} {
		lipgloss.SetHasDarkBackground(true)
		d := render(tc.c)
		lipgloss.SetHasDarkBackground(false)
		l := render(tc.c)
		if d != l {
			t.Errorf("%s should be identical on both backgrounds, got %q vs %q", tc.name, d, l)
		}
	}
}

func TestReplayThemeOverride(t *testing.T) {
	// REPLAY_THEME=light must win even though detection would say dark.
	lipgloss.SetHasDarkBackground(true)
	os.Setenv("REPLAY_THEME", "light")
	applyTheme()
	if lipgloss.HasDarkBackground() {
		t.Errorf("REPLAY_THEME=light did not take effect")
	}

	lipgloss.SetHasDarkBackground(false)
	os.Setenv("REPLAY_THEME", "DARK  ") // case and whitespace tolerant
	applyTheme()
	if !lipgloss.HasDarkBackground() {
		t.Errorf("REPLAY_THEME=DARK (padded) did not take effect")
	}

	// An unset or bogus value must leave detection alone.
	lipgloss.SetHasDarkBackground(true)
	os.Setenv("REPLAY_THEME", "chartreuse")
	applyTheme()
	if !lipgloss.HasDarkBackground() {
		t.Errorf("bogus REPLAY_THEME should not have changed detection")
	}
	os.Unsetenv("REPLAY_THEME")
}

// Renders the real UI and confirms the emitted color codes actually change
// with the background setting.
func TestRenderedViewChangesWithTheme(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<Trace>
<Event Severity="10" Time="1.000000" Type="Role" Machine="2.0.1.0:1" ID="1111111111111111" Transition="Begin" As="StorageServer"/>
<Event Severity="10" Time="1.010000" Type="Role" Machine="2.0.1.0:1" ID="2222222222222222" Transition="Begin" As="TLog"/>
<Event Severity="10" Time="1.100000" Type="StorageMetrics" Machine="2.0.1.0:1" ID="1111111111111111" Version="1234567"/>
<Event Severity="10" Time="1.200000" Type="TLogMetrics" Machine="2.0.1.0:1" ID="2222222222222222" Version="7654321" Generation="5"/>
<Event Severity="10" Time="1.300000" Type="StorageServerSourceTLogID" Machine="2.0.1.0:1" ID="1111111111111111" SourceTLogID="2222222222222222"/>
</Trace>`
	p := t.TempDir() + "/trace.xml"
	if err := os.WriteFile(p, []byte(xml), 0o644); err != nil {
		t.Fatal(err)
	}
	td, err := parseTraceFile(p)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	lipgloss.SetColorProfile(termenv.ANSI256)
	codes := func() map[string]bool {
		m := newModel(td)
		m.width, m.height = 200, 50
		out := m.View()
		found := map[string]bool{}
		for _, f := range strings.Split(out, "\x1b[") {
			if strings.HasPrefix(f, "38;5;") {
				found[strings.SplitN(f, "m", 2)[0]] = true
			}
		}
		return found
	}

	lipgloss.SetHasDarkBackground(true)
	dark := codes()
	lipgloss.SetHasDarkBackground(false)
	light := codes()

	if len(dark) == 0 || len(light) == 0 {
		t.Fatalf("no color codes rendered (dark=%d light=%d)", len(dark), len(light))
	}
	onlyDark, onlyLight := []string{}, []string{}
	for c := range dark {
		if !light[c] {
			onlyDark = append(onlyDark, c)
		}
	}
	for c := range light {
		if !dark[c] {
			onlyLight = append(onlyLight, c)
		}
	}
	sort.Strings(onlyDark)
	sort.Strings(onlyLight)
	t.Logf("dark-only codes : %v", onlyDark)
	t.Logf("light-only codes: %v", onlyLight)
	if len(onlyDark) == 0 || len(onlyLight) == 0 {
		t.Errorf("rendered view did not change with background")
	}
	// The near-white body text must not survive on a light background.
	if light["38;5;252"] {
		t.Errorf("near-white 252 still used on light background")
	}
}

// backgroundIsAGuess mirrors termenv's conditions, so pin them down.
func TestBackgroundIsAGuess(t *testing.T) {
	for _, tc := range []struct {
		term, colorFGBG string
		want            bool
		why             string
	}{
		{"screen-256color", "", true, "inside tmux with no COLORFGBG, dark is assumed"},
		{"tmux-256color", "", true, "the tmux prefix is refused just like screen"},
		{"screen", "", true, "bare screen is refused too"},
		{"xterm-256color", "", false, "outside a multiplexer the query is actually sent"},
		{"screen-256color", "15;0", false, "COLORFGBG gives a real answer inside tmux"},
		{"", "", false, "no TERM is not a multiplexer"},
	} {
		t.Setenv("TERM", tc.term)
		t.Setenv("COLORFGBG", tc.colorFGBG)
		if got := backgroundIsAGuess(); got != tc.want {
			t.Errorf("TERM=%q COLORFGBG=%q: got %v, want %v (%s)",
				tc.term, tc.colorFGBG, got, tc.want, tc.why)
		}
	}
}
