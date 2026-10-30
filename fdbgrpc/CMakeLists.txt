fdb_find_sources(fdbgrpc_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbgrpc
  SRCS ${fdbgrpc_SRCS})
target_include_directories(fdbgrpc PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(fdbgrpc PRIVATE fdbclient)

if (WITH_GRPC)
  generate_grpc_protobuf(fdbgrpc.control_service protos/control_service.proto)
  target_link_libraries(fdbgrpc PUBLIC proto_fdbgrpc_control_service)
endif()

if (USE_UBSAN)
  target_link_options(fdbgrpc PRIVATE "-rdynamic")
endif()
