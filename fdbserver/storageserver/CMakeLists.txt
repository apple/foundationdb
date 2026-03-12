fdb_find_sources(FDBSERVER_STORAGESERVER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_storageserver SRCS ${FDBSERVER_STORAGESERVER_SRCS})

target_include_directories(fdbserver_storageserver
  PRIVATE
    ${CMAKE_SOURCE_DIR}/bindings/c
    ${CMAKE_BINARY_DIR}/bindings/c
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_storageserver PRIVATE fdbclient fdbserver_core)
