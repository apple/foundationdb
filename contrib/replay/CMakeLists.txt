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

# Custom target to build replay
# This handles all Go dependencies automatically - users just need Go installed
# ALL means it builds by default (but only if Go is available - see check above)
add_custom_target(replay ALL
    COMMAND ${CMAKE_COMMAND} -E echo "Downloading Go dependencies..."
    COMMAND ${GO_EXECUTABLE} mod download
    COMMAND ${CMAKE_COMMAND} -E echo "Tidying Go modules..."
    COMMAND ${GO_EXECUTABLE} mod tidy
    COMMAND ${CMAKE_COMMAND} -E echo "Building replay..."
    COMMAND ${GO_EXECUTABLE} build -o ${REPLAY_BINARY} .
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building FDB trace replay tool (all dependencies handled automatically)"
    BYPRODUCTS ${REPLAY_BINARY}
    SOURCES ${REPLAY_GO_SOURCES}
)

# Make sure the binary is executable
add_custom_command(TARGET replay POST_BUILD
    COMMAND chmod +x ${REPLAY_BINARY}
    COMMENT "Making replay executable"
)

message(STATUS "replay will be built by default (Go found)")
message(STATUS "replay binary will be at: ${REPLAY_BINARY}")
