fdb_find_sources(FDBCLI_SRCS)

add_flow_target(EXECUTABLE NAME fdbcli SRCS ${FDBCLI_SRCS})
target_include_directories(fdbcli PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(fdbcli PRIVATE fdbclient SimpleOpt)

if(NOT WIN32)
  target_link_libraries(fdbcli PRIVATE linenoise)
endif()

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbcli DESTINATION bin COMPONENT clients)
  else()
    add_custom_target(prepare_fdbcli_install ALL DEPENDS strip_only_fdbcli)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbcli DESTINATION bin COMPONENT clients)
  endif()
endif()
