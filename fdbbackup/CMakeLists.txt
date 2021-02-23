set(FDBBACKUP_SRCS
  backup.actor.cpp)

add_flow_target(EXECUTABLE NAME fdbbackup SRCS ${FDBBACKUP_SRCS})
target_link_libraries(fdbbackup PRIVATE fdbclient)

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbbackup DESTINATION bin COMPONENT clients)
    fdb_install(PROGRAMS $<fdbbackup:TARGET_FILE> DESTINATION fdbmonitor COMPONENT clients RENAME backup_agent/backup_agent)
    fdb_install(PROGRAMS $<fdbbackup:TARGET_FILE> DESTINATION bin COMPONENT clients RENAME fdbrestore)
    fdb_install(PROGRAMS $<fdbbackup:TARGET_FILE> DESTINATION bin COMPONENT clients RENAME dr_agent)
    fdb_install(PROGRAMS $<fdbbackup:TARGET_FILE> DESTINATION bin COMPONENT clients RENAME fdbdr)
  else()
    add_custom_target(prepare_fdbbackup_install ALL DEPENDS strip_only_fdbbackup)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION fdbmonitor COMPONENT clients RENAME backup_agent/backup_agent)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients RENAME fdbrestore)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients RENAME dr_agent)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients RENAME fdbdr)
  endif()
  symlink_files(
    LOCATION packages/bin
    SOURCE fdbbackup
    TARGETS fdbdr dr_agent backup_agent fdbrestore)
  symlink_files(
    LOCATION bin
    SOURCE fdbbackup
    TARGETS fdbdr dr_agent backup_agent fdbrestore)
endif()

if (GPERFTOOLS_FOUND)
  add_compile_definitions(USE_GPERFTOOLS)
  target_link_libraries(fdbbackup PRIVATE gperftools)
endif()
