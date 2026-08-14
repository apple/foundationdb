package main

import (
	"math"
	"testing"
)

func TestParseOSCColor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      string
		r, g, b float64
		ok      bool
	}{
		{
			// Captured from iTerm2 through tmux passthrough: a light tan.
			name: "iterm2 4-digit reply",
			in:   "\x1b]11;rgb:e980/de59/b850\x1b\\",
			r:    0.9121, g: 0.8686, b: 0.7200, ok: true,
		},
		{
			name: "BEL terminated",
			in:   "\x1b]11;rgb:0000/0000/0000\a",
			r:    0, g: 0, b: 0, ok: true,
		},
		{
			name: "two digit components scale by their own width",
			in:   "\x1b]11;rgb:ff/ff/ff\x1b\\",
			r:    1, g: 1, b: 1, ok: true,
		},
		{
			name: "rgba is accepted and alpha ignored",
			in:   "\x1b]11;rgba:ffff/0000/0000/ffff\x1b\\",
			r:    1, g: 0, b: 0, ok: true,
		},
		{name: "no color at all", in: "\x1b]11;?\x1b\\", ok: false},
		{name: "empty", in: "", ok: false},
		{name: "truncated components", in: "\x1b]11;rgb:ffff/0000\x1b\\", ok: false},
		{name: "not hex", in: "\x1b]11;rgb:zzzz/0000/0000\x1b\\", ok: false},
		{name: "component too wide", in: "\x1b]11;rgb:fffff/0/0\x1b\\", ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, g, b, ok := parseOSCColor(tc.in)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			for _, c := range []struct {
				what      string
				got, want float64
			}{{"r", r, tc.r}, {"g", g, tc.g}, {"b", b, tc.b}} {
				if math.Abs(c.got-c.want) > 0.001 {
					t.Errorf("%s = %.4f, want %.4f", c.what, c.got, c.want)
				}
			}
		})
	}
}

func TestBackgroundIsDark(t *testing.T) {
	for _, tc := range []struct {
		name    string
		r, g, b float64
		want    bool
	}{
		// The real reason this whole thing exists.
		{"iterm2 tan #e9debb", 0.9121, 0.8686, 0.7200, false},
		{"black", 0, 0, 0, true},
		{"white", 1, 1, 1, false},
		{"solarized dark base03", 0, 0.169, 0.212, true},
		{"solarized light base3", 0.992, 0.965, 0.890, false},
		{"mid gray just under half", 0.49, 0.49, 0.49, true},
		{"mid gray just over half", 0.51, 0.51, 0.51, false},
	} {
		if got := backgroundIsDark(tc.r, tc.g, tc.b); got != tc.want {
			t.Errorf("%s: backgroundIsDark(%.3f,%.3f,%.3f) = %v, want %v",
				tc.name, tc.r, tc.g, tc.b, got, tc.want)
		}
	}
}

// Not inside tmux means the query must not be attempted at all, so that
// termenv's own detection stays in charge.
func TestQuerySkippedOutsideTmux(t *testing.T) {
	t.Setenv("TMUX", "")
	if _, _, _, ok := queryBackgroundThroughMultiplexer(); ok {
		t.Errorf("query should not report success outside tmux")
	}
}
