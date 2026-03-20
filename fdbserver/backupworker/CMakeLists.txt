fdb_find_sources(FDBSERVER_BACKUPWORKER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_backupworker SRCS ${FDBSERVER_BACKUPWORKER_SRCS})

configure_fdbserver_common_includes(fdbserver_backupworker)
target_include_directories(fdbserver_backupworker
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fdbserver_backupworker PRIVATE fdbclient fdbserver_core fdbserver_logsystem)
