set(FDBMONITOR_SRCS ConvertUTF.h SimpleIni.h fdbmonitor.cpp)

add_executable(fdbmonitor ${FDBMONITOR_SRCS})
strip_debug_symbols(fdbmonitor)
assert_no_version_h(fdbmonitor)
if(UNIX AND NOT APPLE)
    target_link_libraries(fdbmonitor PRIVATE rt)
endif()
# FIXME: This include directory is an ugly hack. We probably want to fix this
# as soon as we get rid of the old build system
target_link_libraries(fdbmonitor PUBLIC Threads::Threads)

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
