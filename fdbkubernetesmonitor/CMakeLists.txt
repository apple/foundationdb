find_program(FDB_K8S_MONITOR_GO_EXECUTABLE go HINTS /usr/local/go/bin/)
if(NOT FDB_K8S_MONITOR_GO_EXECUTABLE)
  message(WARNING "Go not found. The 'fdb-kubernetes-monitor' target will not be available.")
  return()
endif()

if(WIN32)
  return()
endif()

file(GLOB_RECURSE FDB_K8S_MONITOR_GO_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.go")
set(FDB_K8S_MONITOR_GO_MOD "${CMAKE_CURRENT_SOURCE_DIR}/go.mod")
set(FDB_K8S_MONITOR_GO_SUM "${CMAKE_CURRENT_SOURCE_DIR}/go.sum")

set(FDB_K8S_MONITOR_BINARY "${CMAKE_BINARY_DIR}/bin/fdb-kubernetes-monitor")

add_custom_command(
  OUTPUT ${FDB_K8S_MONITOR_BINARY}
  COMMAND ${FDB_K8S_MONITOR_GO_EXECUTABLE} build -o ${FDB_K8S_MONITOR_BINARY} .
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${FDB_K8S_MONITOR_GO_SOURCES} ${FDB_K8S_MONITOR_GO_MOD} ${FDB_K8S_MONITOR_GO_SUM}
  COMMENT "Building fdb-kubernetes-monitor")

add_custom_target(fdb-kubernetes-monitor ALL
  DEPENDS ${FDB_K8S_MONITOR_BINARY}
  SOURCES ${FDB_K8S_MONITOR_GO_SOURCES})

add_test(
  NAME fdb-kubernetes-monitor-go-tests
  COMMAND ${FDB_K8S_MONITOR_GO_EXECUTABLE} test -race ./...
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# gofmt should be installed with go, but it's better to check if the binary is available.
find_program(FDB_K8S_MONITOR_GOFMT_EXECUTABLE gofmt HINTS /usr/local/go/bin/)
if(FDB_K8S_MONITOR_GOFMT_EXECUTABLE)
  add_test(
    NAME fdb-kubernetes-monitor-go-fmt
    # gofmt -d doesn't return a non-zero exit code, so we have to check if the output is empty.
    COMMAND bash -c "out=$(\"${FDB_K8S_MONITOR_GOFMT_EXECUTABLE}\" -d \"${CMAKE_CURRENT_SOURCE_DIR}\"); test -z \"$out\" || { echo \"$out\"; exit 1; }"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
else()
  message(WARNING "gofmt not found. The 'fdb-kubernetes-monitor-go-fmt' test will not be available.")
endif()
