fdb_find_sources(FDBSERVER_CORE_LOCAL_SRCS)
add_flow_target(STATIC_LIBRARY NAME fdbserver_core SRCS ${FDBSERVER_CORE_LOCAL_SRCS})

configure_fdbserver_common_includes(fdbserver_core)
target_include_directories(fdbserver_core
  PUBLIC
    ${CMAKE_SOURCE_DIR}/fdbserver/include
    ${CMAKE_BINARY_DIR}/fdbserver/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include)
target_link_libraries(fdbserver_core PRIVATE fdbclient)
