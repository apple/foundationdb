set(FDBMONITOR_SRCS fdbmonitor.cpp)

add_executable(fdbmonitor ${FDBMONITOR_SRCS})
strip_debug_symbols(fdbmonitor)
assert_no_version_h(fdbmonitor)
if(UNIX AND NOT APPLE)
    target_link_libraries(fdbmonitor PRIVATE rt)
endif()
# FIXME: This include directory is an ugly hack. We probably want to fix this
# as soon as we get rid of the old build system
target_link_libraries(fdbmonitor PUBLIC Threads::Threads)

# We don't compile fdbmonitor with thread sanitizer instrumentation, since this
# appears to change its behavior (it no longer seems to restart killed
# processes). fdbmonitor is single-threaded anyway.
get_target_property(fdbmonitor_options fdbmonitor COMPILE_OPTIONS)
list(REMOVE_ITEM fdbmonitor_options "-fsanitize=thread")
set_property(TARGET fdbmonitor PROPERTY COMPILE_OPTIONS ${target_options})
get_target_property(fdbmonitor_options fdbmonitor LINK_OPTIONS)
list(REMOVE_ITEM fdbmonitor_options "-fsanitize=thread")
set_property(TARGET fdbmonitor PROPERTY LINK_OPTIONS ${target_options})

if(GENERATE_DEBUG_PACKAGES)
  fdb_install(TARGETS fdbmonitor DESTINATION fdbmonitor COMPONENT server)
else()
  add_custom_target(prepare_fdbmonitor_install ALL DEPENDS strip_only_fdbmonitor)
  fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbmonitor DESTINATION fdbmonitor COMPONENT server)
endif()

# Create a local sandbox for quick manual testing without simulator
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/sandbox/data)
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/sandbox/logs)
if(NOT EXISTS ${CMAKE_BINARY_DIR}/sandbox/foundationdb.conf)
  configure_file(${CMAKE_SOURCE_DIR}/cmake/Sandbox.conf.cmake
    ${CMAKE_BINARY_DIR}/sandbox/foundationdb.conf)
endif()

# this is not portable on Windows - but fdbmonitor isn't built there anyways...
add_custom_target(clean_sandbox
  COMMAND rm -rf ${CMAKE_BINARY_DIR}/sandbox/data/* && rm -rf ${CMAKE_BINARY_DIR}/sandbox/logs/*
  COMMENT "Cleaning existing sandbox")

add_custom_target(start_sandbox
  COMMAND ${CMAKE_BINARY_DIR}/bin/fdbmonitor --conffile ${CMAKE_BINARY_DIR}/sandbox/foundationdb.conf
                                             --lockfile ${CMAKE_BINARY_DIR}/sandbox/fdbmonitor.lock)

add_dependencies(start_sandbox fdbmonitor fdbserver)

if(NOT EXISTS ${CMAKE_BINARY_DIR}/contrib/generate_profile.sh)
  configure_file(${CMAKE_SOURCE_DIR}/contrib/generate_profile.sh
    ${CMAKE_BINARY_DIR}/contrib/generate_profile.sh)
endif()

add_custom_target(generate_profile
  COMMAND  ${CMAKE_BINARY_DIR}/contrib/generate_profile.sh ${CMAKE_BINARY_DIR})

add_dependencies(generate_profile fdbmonitor fdbserver mako fdbcli)
