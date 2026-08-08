package main

import (
	"errors"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/term"
)

// The terminal answers in a few milliseconds when it answers at all, so this is
// generous. It is also the delay paid at startup when nothing answers, which is
// why it is not longer.
const backgroundQueryTimeout = 250 * time.Millisecond

// queryBackgroundThroughMultiplexer asks the outer terminal what its background
// color is, from inside tmux.
//
// termenv deliberately never sends the OSC 11 background query when TERM starts
// with "screen" or "tmux", because a multiplexer can be attached from several
// terminals at once with different backgrounds. That leaves $COLORFGBG, and
// failing that an assumption of black, so inside tmux the answer is always
// "dark" however the terminal actually looks.
//
// tmux will forward a sequence verbatim to the terminal it is attached to if the
// sequence is wrapped in its DCS passthrough, and the reply comes back on the
// same tty. So the question is answerable after all, as long as it is addressed
// to the outer terminal rather than to tmux.
//
// Returns ok=false when there is nothing to ask, when the terminal does not
// answer, or when the answer cannot be parsed. Callers should fall back to
// whatever they would have done otherwise.
func queryBackgroundThroughMultiplexer() (r, g, b float64, ok bool) {
	// Outside a multiplexer termenv's own detection works, so leave it alone.
	if os.Getenv("TMUX") == "" {
		return 0, 0, 0, false
	}

	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return 0, 0, 0, false
	}
	defer tty.Close()

	fd := int(tty.Fd())
	if !term.IsTerminal(fd) {
		return 0, 0, 0, false
	}

	// The reply is escape bytes with no newline: canonical mode would not return
	// it until a newline arrived, and echo would paint it on the screen.
	state, err := term.MakeRaw(fd)
	if err != nil {
		return 0, 0, 0, false
	}
	defer term.Restore(fd, state) //nolint:errcheck

	const osc11 = "\x1b]11;?\x1b\\"
	// Inside the passthrough every ESC of the payload has to be doubled.
	payload := strings.ReplaceAll(osc11, "\x1b", "\x1b\x1b")
	if _, err := tty.WriteString("\x1bPtmux;" + payload + "\x1b\\"); err != nil {
		return 0, 0, 0, false
	}

	reply, err := readTerminalReply(tty, backgroundQueryTimeout)
	if err != nil {
		return 0, 0, 0, false
	}
	return parseOSCColor(reply)
}

// readTerminalReply reads until the reply looks terminated or the deadline
// passes.
//
// The read has to be bounded by a deadline rather than by the terminal driver's
// VMIN/VTIME: Go opens the tty non-blocking and waits on it with its poller, so
// a VTIME timeout never fires and the read would block indefinitely.
func readTerminalReply(tty *os.File, budget time.Duration) (string, error) {
	if err := tty.SetReadDeadline(time.Now().Add(budget)); err != nil {
		return "", err
	}
	var got []byte
	for {
		var buf [64]byte
		n, err := tty.Read(buf[:])
		got = append(got, buf[:n]...)
		if err != nil {
			// A deadline with something already buffered is still usable.
			if errors.Is(err, os.ErrDeadlineExceeded) && len(got) > 0 {
				return string(got), nil
			}
			return string(got), err
		}
		s := string(got)
		if strings.Contains(s, "\a") || strings.HasSuffix(s, "\x1b\\") {
			return s, nil
		}
		// Do not read forever if the terminator never shows up.
		if len(got) > 1024 {
			return s, nil
		}
	}
}

// parseOSCColor pulls the color out of an OSC 11 reply, which looks like
//
//	ESC ] 11 ; rgb:e980/de59/b850 ESC \
//
// Each component is hex and may be one to four digits, scaled against its own
// width, so "e980" and "e9" both mean roughly 0.91. Some terminals answer
// "rgba:" with a fourth component, which is ignored.
func parseOSCColor(s string) (r, g, b float64, ok bool) {
	var rest string
	switch {
	case strings.Contains(s, "rgba:"):
		rest = s[strings.Index(s, "rgba:")+len("rgba:"):]
	case strings.Contains(s, "rgb:"):
		rest = s[strings.Index(s, "rgb:")+len("rgb:"):]
	default:
		return 0, 0, 0, false
	}

	// Drop the terminator, whether ST or BEL.
	if i := strings.IndexAny(rest, "\x1b\a"); i >= 0 {
		rest = rest[:i]
	}

	parts := strings.Split(rest, "/")
	if len(parts) < 3 {
		return 0, 0, 0, false
	}
	var out [3]float64
	for i := 0; i < 3; i++ {
		p := strings.TrimSpace(parts[i])
		if p == "" || len(p) > 4 {
			return 0, 0, 0, false
		}
		v, err := strconv.ParseUint(p, 16, 32)
		if err != nil {
			return 0, 0, 0, false
		}
		out[i] = float64(v) / float64(int(1)<<(4*len(p))-1)
	}
	return out[0], out[1], out[2], true
}

// backgroundIsDark applies the same rule termenv does: HSL lightness below a
// half counts as dark.
func backgroundIsDark(r, g, b float64) bool {
	max := math.Max(r, math.Max(g, b))
	min := math.Min(r, math.Min(g, b))
	return (max+min)/2 < 0.5
}
