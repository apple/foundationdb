fdb_find_sources(FDBCTL_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbctl
  SRCS ${FDBCTL_SRCS})
target_include_directories(fdbctl PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(fdbctl PRIVATE fdbclient)

if (WITH_GRPC)
  generate_grpc_protobuf(fdbctl.control_service protos/control_service.proto)
  target_link_libraries(fdbctl PUBLIC proto_fdbctl_control_service)
endif()

if (USE_UBSAN)
  target_link_options(fdbctl PRIVATE "-rdynamic")
endif()
