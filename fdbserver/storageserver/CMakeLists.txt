fdb_find_sources(FDBSERVER_STORAGESERVER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_storageserver SRCS ${FDBSERVER_STORAGESERVER_SRCS})
add_fdbserver_link_test(fdbserver_storageserverlinktest
  fdbserver_storageserver
  fdbserver_logsystem
  fdbserver_kvstore
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_storageserver)
target_include_directories(fdbserver_storageserver
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fdbserver_storageserver PRIVATE fdbserver_core fdbserver_kvstore fdbserver_logsystem)
