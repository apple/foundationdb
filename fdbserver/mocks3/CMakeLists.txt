fdb_find_sources(FDBSERVER_MOCKS3_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_mocks3 SRCS ${FDBSERVER_MOCKS3_SRCS})

configure_fdbserver_common_includes(fdbserver_mocks3)
target_include_directories(fdbserver_mocks3
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(fdbserver_mocks3 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/rapidjson)
target_link_libraries(fdbserver_mocks3 PRIVATE fdbclient)
