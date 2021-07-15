set(COMMON_BACKUP_SRCS
 BackupTLSConfig.cpp
 BackupTLSConfig.h
 BackupRestoreCommon.cpp
 BackupRestoreCommon.h
)

set(FDB_BACKUP_SRCS
  ${COMMON_BACKUP_SRCS}
  backup.actor.cpp)

set(FDB_RESTORE_SRCS
  ${COMMON_BACKUP_SRCS}
  RestoreDriver.cpp
  RestoreDriver.h
  fdbrestore.actor.cpp)

set(FDB_BACKUP_AGENT_SRCS
  ${COMMON_BACKUP_SRCS}
  AgentDriver.actor.cpp
  AgentDriver.h
  backup_agent.actor.cpp)

set(FDB_DR_AGENT_SRCS
  ${COMMON_BACKUP_SRCS}
  AgentDriver.actor.cpp
  AgentDriver.h
  dr_agent.actor.cpp)

set(FDB_FAST_RESTORE_SRCS
  ${COMMON_BACKUP_SRCS}
  RestoreDriver.cpp
  RestoreDriver.h
  fastrestore_tool.actor.cpp)

add_flow_target(EXECUTABLE NAME fdbbackup SRCS ${FDB_BACKUP_SRCS})
add_flow_target(EXECUTABLE NAME fdbrestore SRCS ${FDB_RESTORE_SRCS})
add_flow_target(EXECUTABLE NAME backup_agent SRCS ${FDB_BACKUP_AGENT_SRCS})
add_flow_target(EXECUTABLE NAME dr_agent SRCS ${FDB_DR_AGENT_SRCS})
add_flow_target(EXECUTABLE NAME fastrestore_tool SRCS ${FDB_FAST_RESTORE_SRCS})
target_link_libraries(fdbbackup PRIVATE fdbclient)
target_link_libraries(fdbrestore PRIVATE fdbclient)
target_link_libraries(backup_agent PRIVATE fdbclient)
target_link_libraries(dr_agent PRIVATE fdbclient)
target_link_libraries(fastrestore_tool PRIVATE fdbclient)

set(FDBCONVERT_SRCS
	FileConverter.actor.cpp
	FileConverter.h)
add_flow_target(EXECUTABLE NAME fdbconvert SRCS ${FDBCONVERT_SRCS})
target_link_libraries(fdbconvert PRIVATE fdbclient)

set(FDBDECODE_SRCS
	BackupTLSConfig.h
	BackupTLSConfig.cpp
	FileDecoder.actor.cpp
	FileConverter.h)
add_flow_target(EXECUTABLE NAME fdbdecode SRCS ${FDBDECODE_SRCS})
target_link_libraries(fdbdecode PRIVATE fdbclient)

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbbackup DESTINATION bin COMPONENT clients)
    fdb_install(PROGRAMS $<TARGET_FILE:fdbbackup> DESTINATION bin COMPONENT clients RENAME fdbdr)
  else()
    add_custom_target(prepare_fdbbackup_install ALL DEPENDS strip_only_fdbbackup)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients RENAME fdbdr)
  endif()
  symlink_files(
    LOCATION packages/bin
    SOURCE fdbbackup
    TARGETS fdbdr)
  symlink_files(
    LOCATION bin
    SOURCE fdbbackup
    TARGETS fdbdr)
endif()

if (GPERFTOOLS_FOUND)
  add_compile_definitions(USE_GPERFTOOLS)
  target_link_libraries(fdbbackup PRIVATE gperftools)
endif()
