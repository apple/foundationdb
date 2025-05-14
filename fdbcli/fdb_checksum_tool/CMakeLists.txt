# CMakeLists.txt for fdb_checksum_tool

# Source file for the tool
set(TOOL_SRCS fdb_checksum_tool.actor.cpp)

add_flow_target(EXECUTABLE NAME fdb_checksum_tool SRCS ${TOOL_SRCS})

target_compile_definitions(fdb_checksum_tool PRIVATE WITH_ROCKSDB)

# Include directories. All FDB-specific includes will be relative to CMAKE_SOURCE_DIR.
set(FDB_SOURCE_DIR ${CMAKE_SOURCE_DIR})

target_include_directories(fdb_checksum_tool PUBLIC
    # For fdbclient headers (e.g., NativeAPI.actor.h)
    "${CMAKE_SOURCE_DIR}/fdbclient/include"
    "${CMAKE_BINARY_DIR}/fdbclient/include"

    # For flow headers (e.g., flow.h, actorcompiler.h)
    "${CMAKE_SOURCE_DIR}/flow/include"
    "${CMAKE_BINARY_DIR}/flow/include"

    # For fdbrpc headers
    "${CMAKE_SOURCE_DIR}/fdbrpc/include"
    "${CMAKE_BINARY_DIR}/fdbrpc/include"

    # For xxhash.h directly from flow source if not in flow/include
    "${CMAKE_SOURCE_DIR}/flow"

    # Add C binding directories used by fdbclient
    "${CMAKE_SOURCE_DIR}/bindings/c/foundationdb"
    "${CMAKE_BINARY_DIR}/bindings/c/foundationdb"

    # For fmt library
    ${FMT_INCLUDE_DIRS}

    # For Boost headers
    "${CMAKE_BINARY_DIR}/boost_install/include"
)

# Link libraries
target_link_libraries(fdb_checksum_tool PRIVATE
    fdbclient
    fdbrpc
    flow
    fdb_flow
    "${CMAKE_BINARY_DIR}/boost_install/lib/libboost_program_options.a"
    "${CMAKE_BINARY_DIR}/boost_install/lib/libboost_system.a"
    "${CMAKE_BINARY_DIR}/boost_install/lib/libboost_filesystem.a"
    stacktrace
    fmt::fmt
)

# Add fdbclient as a dependency to ensure its generated headers are available
add_dependencies(fdb_checksum_tool fdboptions fdbclient)

# Installation rules
if(NOT OPEN_FOR_IDE AND FDB_INSTALL_PROGRAMS)
  fdb_install(TARGETS fdb_checksum_tool DESTINATION bin COMPONENT tools)
endif() 