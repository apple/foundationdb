fdb_find_sources(FDBCLI_LIB_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbcli_lib
  SRCS ${FDBCLI_LIB_SRCS})
target_include_directories(fdbcli_lib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(fdbcli_lib PRIVATE fdbclient)

if (WITH_GRPC)
  generate_grpc_protobuf(fdbcli_lib.cli_service protos/cli_service.proto)
  target_link_libraries(fdbcli_lib PUBLIC proto_fdbcli_lib_cli_service)
endif()

if (USE_UBSAN)
  target_link_options(fdbcli_lib PRIVATE "-rdynamic")
endif()
