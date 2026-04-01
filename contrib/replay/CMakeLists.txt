# replay - Go-based TUI tool for FDB trace files
cmake_minimum_required(VERSION 3.13)

# Find Go (required for building replay)
find_program(GO_EXECUTABLE go)
if(NOT GO_EXECUTABLE)
    message(WARNING "Go not found. The 'replay' target will not be available. Install Go 1.21+ to build replay.")
    return()
endif()

# Verify Go version
execute_process(
    COMMAND ${GO_EXECUTABLE} version
    OUTPUT_VARIABLE GO_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "Found Go: ${GO_VERSION_OUTPUT}")

# Set output directory for the binary
set(REPLAY_OUTPUT_DIR "${CMAKE_BINARY_DIR}/bin")
set(REPLAY_BINARY "${REPLAY_OUTPUT_DIR}/replay")

# Ensure output directory exists
file(MAKE_DIRECTORY ${REPLAY_OUTPUT_DIR})

# Get all Go source files for dependency tracking
file(GLOB REPLAY_GO_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.go")
set(REPLAY_GO_MOD "${CMAKE_CURRENT_SOURCE_DIR}/go.mod")
set(REPLAY_GO_SUM "${CMAKE_CURRENT_SOURCE_DIR}/go.sum")

# Custom command to build replay - only rebuilds when source files change
# go build handles dependencies automatically (downloads if needed)
add_custom_command(
    OUTPUT ${REPLAY_BINARY}
    COMMAND ${GO_EXECUTABLE} build -o ${REPLAY_BINARY} .
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${REPLAY_GO_SOURCES} ${REPLAY_GO_MOD} ${REPLAY_GO_SUM}
    COMMENT "Building FDB trace replay tool"
)

# Target that depends on the binary - only triggers rebuild when deps change
add_custom_target(replay ALL
    DEPENDS ${REPLAY_BINARY}
    SOURCES ${REPLAY_GO_SOURCES}
)

message(STATUS "replay will be built by default (Go found)")
message(STATUS "replay binary will be at: ${REPLAY_BINARY}")
