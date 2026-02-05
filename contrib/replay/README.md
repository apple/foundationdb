# replay

```
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
           Time Travel Wizard
```

An interactive Terminal User Interface (TUI) for "replaying" FoundationDB simulation trace files.

## Disclaimer

**This tool is in EXPERIMENTAL stage.**

- **Bugs likely exist** - this is early software, expect rough edges
- **Performance could be improved** - there's room for optimization
- **Interface can change** - keyboard shortcuts, UI layout, and features may evolve significantly based on user feedback

I wanted to create a tool that I personally always wanted in FDB. Tired of grepping trace files and doing common patterns again and again, I wanted to codify the general techniques I use and data I look at while debugging failures - just making it very fast and easily available, while not regressing on core grep/less functionality.

At the end, the goal is to make myself more productive, learn about FDB better, and in the future, potentially for others if they find this tool useful too.

## Building

**Prerequisites:** Go 1.21+ must be installed on the system.

If Go is installed, `replay` is built automatically as part of the default FDB build. If Go is not installed, CMake will emit a warning during configuration and skip the `replay` target. The rest of the FDB build will proceed normally.

### Using CMake

The `replay` binary is built by default when you build FDB:

```bash
cmake --build .          # replay is included in the default build
cmake --build . --target replay   # or build replay specifically
```

CMake handles everything automatically:
- Downloads all Go dependencies
- Builds the binary
- Places it in `bin/replay`

### Manual Build

If you prefer to build directly with Go:

```bash
cd contrib/replay
go build -o replay .
```

### Usage

```bash
replay [trace-file.xml]    # Load specific trace file
replay                     # Auto-load latest trace*.xml in current directory
replay -h | --help         # Show help
```

**Tip:** Create an alias `r` for quick access:
```bash
alias r=replay
```

## Why

The main motivation is to be **fast in debugging simulation issues**, as well as **understanding how FDB works**.

FDB simulation produces XML trace files that can be gigabytes in size with millions of events. Debugging a test failure typically involves:
- Grepping for specific event types
- Manually correlating events across time
- Tracking down which machines had which roles at a given moment
- Understanding recovery sequences and epoch boundaries
- Finding the needle in the haystack of millions of trace events

This tool aims to make all of that faster and more intuitive.

## Organic Development

This tool is meant to be **organic** - it will grow and evolve over time. There will always be features to add, improvements to make, and new patterns to codify.

**Feature Wishlist (always growing):**
- More navigation shortcuts for common patterns
- Better visualization of specific subsystems
- Performance optimizations for very large traces
- Additional filter types and search capabilities

**Opportunity for Richer Logging:**

Building this tool has revealed opportunities to improve FDB's trace logging itself. For example, I realized some trace events don't include the role ID, which is very useful since you can then see which role (e.g., ClusterController) the trace event is coming from, or even better, which specific StorageServer (out of dozens in simulation) the trace is coming from.

The tool and the logging can evolve together - as we add features to the tool, we may discover places where richer trace events would help debugging.

## Core Idea / Principles

### File/Text Based
- Takes **only XML trace files** as input (the files produced by FDB simulation)
- No external dependencies, no database, no network - just the trace file itself
- Single low-dependency binary that runs anywhere

### Time-Based Navigation
The core idea is to **navigate the trace file** and then, wherever you are, **"as of" that time/line, the tool reflects the state of the cluster**.

Everything is based on where you are in the timeline:
- Cluster topology (which machines exist, what roles they have)
- DB configuration
- Recovery state
- Epoch/version information

You can go back and forth in time, and the tool **reacts to that** - showing you the cluster state at that exact moment.

From experience debugging FDB issues, you have to do multiple rounds of "back and forth" like this to narrow down the root cause of a bug.

### Terminal UI (TUI) over GUI
- **No dependencies** apart from the trace file itself
- Runs on Linux environments **right inside the shell** where FDB devs run sim tests and inspect trace files
- Works over SSH, in containers, on any terminal

### Why Go?
- **Excellent TUI library** (Bubbletea/Charmbracelet ecosystem)
- **Static typing** catches bugs early
- **Static binary** - no runtime dependencies
- **Potential for concurrency** if performance optimization is needed in the future

## Features

### Navigation

| Key | Action | Description |
|-----|--------|-------------|
| `Ctrl+N` | Next event | Jump to the next visible event (respects filters) |
| `Ctrl+P` | Previous event | Jump to the previous visible event |
| `Ctrl+V` | Page forward | Jump ~1 second forward in time |
| `Alt+V` | Page backward | Jump ~1 second backward in time |
| `g` | Go to start | Jump to the first visible event |
| `G` / `Shift+G` | Go to end | Jump to the last visible event |
| `t` | Time jump | Open popup to enter a specific time in seconds |

### Search

| Key | Action | Description |
|-----|--------|-------------|
| `/` | Search forward | Enter search pattern (supports `*` wildcard) |
| `?` | Search backward | Enter search pattern (supports `*` wildcard) |
| `n` | Next match | Go to next match in original search direction |
| `N` / `Shift+N` | Previous match | Go to match in opposite direction |
| `Esc` | Clear search | Clear search highlighting |

Search patterns support wildcards: `*Recovery*`, `Type=Master*`, `Machine=2.0.1.*`

### Recovery Navigation

| Key | Action | Description |
|-----|--------|-------------|
| `r` | Next recovery start | Jump to next `MasterRecoveryState` with `StatusCode=0` |
| `R` / `Shift+R` | Previous recovery start | Jump to previous recovery start |
| `e` | Next recovery event | Jump to any next `MasterRecoveryState` event |
| `E` / `Shift+E` | Previous recovery event | Jump to any previous recovery event |

Recovery states are color-coded:
- **Red**: StatusCode < 11 (early recovery)
- **Blue**: StatusCode 11-13 (mid recovery)
- **Green**: StatusCode = 14 (fully recovered)

### Severity Navigation

| Key | Action | Description |
|-----|--------|-------------|
| `3` | Next warning | Jump to next `Severity=30` event |
| `#` / `Shift+3` | Previous warning | Jump to previous warning |
| `4` | Next error | Jump to next `Severity=40` event |
| `$` / `Shift+4` | Previous error | Jump to previous error |

### Filtering

Press `f` to open the filter configuration popup. Filters allow you to focus on specific events.

**Filter Categories:**

1. **Raw Filters** (pattern matching)
   - Wildcard patterns like `Type=MasterRecoveryState`, `Role*TLog`, `Severity=40`
   - Multiple patterns use OR logic (any match shows the event)
   - Press `a` to add, `e` to edit, `r` to remove, `d` to disable/enable
   - Press `t` to search and add by Type name (fuzzy search)
   - Press `c` to toggle "common" event types (pre-defined important events)

2. **Machine Filters**
   - Select specific machines or entire data centers
   - Press `Enter` to open machine selection popup
   - Supports fuzzy search to find machines quickly

3. **Time Range Filter**
   - Show only events within a time window
   - Press `d` to toggle, `Enter` to configure start/end times

4. **Message Filter**
   - Show only `NetworkMessageSent` events
   - Press `d` to toggle

**Filter Logic:**
- Categories combine with **AND** logic (event must match all active categories)
- Within each category, patterns use **OR** logic (event can match any pattern)
- Toggle "All" with `Space` to disable/enable all filtering

### Views / Popups

| Key | Action | Description |
|-----|--------|-------------|
| `c` | Config view | Show full DB configuration JSON (scrollable) |
| `x` | Health view | Show network latencies, degraded peers, connections |
| `h` | Help | Show all keyboard shortcuts |
| `f` | Filter | Configure event filters |

**Config View (`c`):**
- Shows full JSON configuration at current time
- Scrollable with `Ctrl+N`/`Ctrl+P`
- Displays redundancy mode, log count, storage engine, etc.

**Health View (`x`):**
- **Network Latencies**: PingLatency events showing min/max/median/P90 latencies
- **Degraded Peers**: HealthMonitorDetectDegradedPeer events
- **Connections**: Sim2Connection and SimulatedDisconnection events
- Shows top 5 entries per category, sorted by latency

### Topology Display

The left pane shows the **cluster topology** at the current time:
- Machines grouped by Data Center (DC)
- Testers shown separately
- Each machine shows its roles (StorageServer, TLog, Coordinator, etc.)
- Roles show ID and epoch where applicable: `TLog [abc123] (e=5)`
- Current event's machine is highlighted with `->` arrow
- Current event's role is highlighted if ID matches

**NetworkMessageSent Visualization:**
- Source machine highlighted with yellow background and `-->` arrow
- Destination machine highlighted with `<--` arrow
- RPC name shown at bottom of topology pane

### Status Bar

The bottom of the screen shows:
- **DB Config**: Current database configuration summary
- **Recovery State**: Current recovery status with color coding
- **Epoch Info**: Current epoch, KCV, RV, recoveryTxnVersion
- **Time**: Current position in the trace timeline

### General

| Key | Action |
|-----|--------|
| `q` / `Q` / `Ctrl+C` | Quit |

## Architecture

```
                                    +------------------+
                                    |   trace*.xml     |
                                    |  (FDB sim trace) |
                                    +--------+---------+
                                             |
                                             v
+-----------------------------------------------------------------------------------+
|                                      main.go                                       |
|  - CLI argument parsing                                                            |
|  - Auto-find latest trace*.xml if not specified                                    |
|  - Load and parse trace file                                                       |
|  - Launch TUI                                                                      |
+-----------------------------------------------------------------------------------+
                                             |
                                             v
+-----------------------------------------------------------------------------------+
|                                     trace.go                                       |
|  - XML parsing (streaming decoder)                                                 |
|  - TraceEvent struct (Time, Type, Machine, ID, Severity, Attrs)                   |
|  - DBConfig parsing from MasterRecoveryState events                               |
|  - RecoveryState tracking                                                          |
|  - EpochVersionInfo tracking (from GetDurableResult, UpdateRegistration)          |
|  - Binary search for time-based lookups                                            |
+-----------------------------------------------------------------------------------+
                                             |
                                             v
+-----------------------------------------------------------------------------------+
|                                    cluster.go                                      |
|  - Worker/RoleInfo structs                                                         |
|  - BuildClusterState() - reconstructs topology from Role events                   |
|  - Address parsing (DC extraction, main vs tester)                                |
|  - Epoch tracking from TLogStart, LogRouterStart, BackupWorkerStart              |
+-----------------------------------------------------------------------------------+
                                             |
                                             v
+-----------------------------------------------------------------------------------+
|                                      ui.go                                         |
|  - Bubbletea TUI framework (Model-Update-View pattern)                            |
|  - Split-pane layout: Topology (left) | Events (right)                            |
|  - Popup overlays: Help, Config, Health, Filter, Time Jump                        |
|  - Navigation handlers (Ctrl+N/P, g/G, r/R, e/E, etc.)                            |
|  - Search with wildcard support                                                    |
|  - Filter system (Raw, Machine, Time, Message)                                    |
|  - NetworkMessageSent visualization                                                |
+-----------------------------------------------------------------------------------+
                                             |
                                             v
                                    +------------------+
                                    |    Terminal      |
                                    |   (user sees)    |
                                    +------------------+
```

### Component Responsibilities

**main.go** (~120 lines)
- Entry point and CLI parsing
- Auto-finds latest `trace*.xml` if no argument provided
- Coordinates parsing and TUI launch

**trace.go** (~515 lines)
- **TraceEvent**: Core data structure for each trace line
- **TraceData**: Container for all parsed data with time-based accessors
- **DBConfig**: Parsed database configuration from MasterRecoveryState
- **RecoveryState**: Recovery milestones with event indices
- **EpochVersionInfo**: Epoch/version tracking from multiple event types
- XML streaming parser with progress reporting
- Binary search for all time-based lookups

**cluster.go** (~265 lines)
- **Worker**: Machine with roles and DC membership
- **RoleInfo**: Role name, ID, and epoch
- **ClusterState**: Map of all workers
- **BuildClusterState()**: Replays Role events to reconstruct topology
- Address parsing for DC identification (handles both IPv4 and IPv6 formats)

**ui.go** (~4600 lines)
- **model**: All TUI state (current position, filters, popup modes)
- **Update()**: Handles all keyboard input
- **View()**: Renders the full screen
- Multi-column topology layout
- Event list with wrapping and scrolling
- All popup rendering (help, config, health, filter, time jump)
- Search and filter matching logic
- NetworkMessageSent visualization

### Data Flow

1. **Parse**: XML trace file -> TraceEvent array (sorted by time)
2. **Index**: Build DBConfig, RecoveryState, EpochVersionInfo arrays
3. **Navigate**: User moves through events via keyboard
4. **Reconstruct**: On each navigation, rebuild ClusterState from events[0:current]
5. **Render**: Display topology + events + status at current position

### Performance Optimizations

- **Streaming XML parser**: Doesn't load entire file into memory
- **Pre-allocation**: Event array sized based on file size estimate
- **Binary search**: All time-based lookups use binary search
- **Pre-compiled regex**: Filter patterns compiled once, cached
- **Set lookups**: Machine filters use O(1) map lookups
- **DC caching**: Machine-to-DC mapping cached per session
- **Progress reporting**: Every 100K events during loading

## Dependencies

Go dependencies are managed automatically by the build system. For reference:

- Go 1.21+
- [Bubbletea](https://github.com/charmbracelet/bubbletea) v0.25.0 - TUI framework
- [Lipgloss](https://github.com/charmbracelet/lipgloss) v0.9.1 - Styling
- [Bubbles](https://github.com/charmbracelet/bubbles) v0.18.0 - Text input widgets
