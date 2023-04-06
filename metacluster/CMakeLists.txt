fdb_find_sources(METACLUSTER_SRCS)

make_directory(${CMAKE_CURRENT_BINARY_DIR}/include/metacluster/)

################################################################################
# Build information
################################################################################

add_flow_target(STATIC_LIBRARY NAME metacluster SRCS ${METACLUSTER_SRCS})
target_include_directories(metacluster PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(metacluster PUBLIC fdbclient)

