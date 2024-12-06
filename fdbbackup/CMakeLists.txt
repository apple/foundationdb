set(FDBBACKUP_SRCS
	Decode.cpp
	backup.actor.cpp)

add_flow_target(EXECUTABLE NAME fdbbackup SRCS ${FDBBACKUP_SRCS})
target_include_directories(fdbbackup PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(fdbbackup PRIVATE fdbclient)

set(FDBCONVERT_SRCS
	FileConverter.actor.cpp)
add_flow_target(EXECUTABLE NAME fdbconvert SRCS ${FDBCONVERT_SRCS})
target_include_directories(fdbconvert PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(fdbconvert PRIVATE fdbclient)

set(FDBDECODE_SRCS
	Decode.cpp
	FileDecoder.actor.cpp)
add_flow_target(EXECUTABLE NAME fdbdecode SRCS ${FDBDECODE_SRCS})
target_include_directories(fdbdecode PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(fdbdecode PRIVATE fdbclient)

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbbackup DESTINATION bin COMPONENT clients)
    fdb_install(PROGRAMS $<TARGET_FILE:fdbbackup> DESTINATION backupagent COMPONENT clients RENAME backup_agent/backup_agent)
    fdb_install(PROGRAMS $<TARGET_FILE:fdbbackup> DESTINATION bin COMPONENT clients RENAME fdbrestore)
    fdb_install(PROGRAMS $<TARGET_FILE:fdbbackup> DESTINATION bin COMPONENT clients RENAME dr_agent)
    fdb_install(PROGRAMS $<TARGET_FILE:fdbbackup> DESTINATION bin COMPONENT clients RENAME fdbdr)
  else()
    add_custom_target(prepare_fdbbackup_install ALL DEPENDS strip_only_fdbbackup)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION backupagent COMPONENT clients RENAME backup_agent/backup_agent)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients RENAME fdbrestore)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients RENAME dr_agent)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients RENAME fdbdr)
  endif()
  symlink_files(
    LOCATION packages/bin
    SOURCE fdbbackup
    TARGETS fdbdr dr_agent backup_agent fdbrestore fastrestore_tool)
  symlink_files(
    LOCATION bin
    SOURCE fdbbackup
    TARGETS fdbdr dr_agent backup_agent fdbrestore fastrestore_tool)
endif()

if (GPERFTOOLS_FOUND)
  add_compile_definitions(USE_GPERFTOOLS)
  target_link_libraries(fdbbackup PRIVATE gperftools)
endif()

if (NOT WIN32 AND NOT OPEN_FOR_IDE)
  enable_testing()
  add_test(NAME dir_backup_tests COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/dir_backup_test.sh ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
  add_test(NAME s3_backup_tests COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/s3_backup_test.sh ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
endif()
