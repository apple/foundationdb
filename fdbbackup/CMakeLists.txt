set(FDBBACKUP_SRCS
  backup.actor.cpp)

add_flow_target(EXECUTABLE NAME fdbbackup SRCS ${FDBBACKUP_SRCS})
target_link_libraries(fdbbackup PRIVATE fdbclient)

if(NOT OPEN_FOR_IDE)
  install(TARGETS fdbbackup DESTINATION ${FDB_BIN_DIR} COMPONENT clients)
  install_symlink(
    TO /${FDB_BIN_DIR}/fdbbackup
    DESTINATION ${FDB_LIB_DIR}/foundationdb/backup_agent/backup_agent
    COMPONENT clients)
  install_symlink(
    TO /${FDB_BIN_DIR}/fdbbackup
    DESTINATION ${FDB_BIN_DIR}/fdbrestore
    COMPONENT clients)
  install_symlink(
    TO /${FDB_BIN_DIR}/fdbbackup
    DESTINATION ${FDB_BIN_DIR}/dr_agent
    COMPONENT clients)
  install_symlink(
    TO /${FDB_BIN_DIR}/fdbbackup
    DESTINATION ${FDB_BIN_DIR}/fdbdr
    COMPONENT clients)
endif()
