set(FDBBACKUP_SRCS
  backup.actor.cpp)

add_flow_target(EXECUTABLE NAME fdbbackup SRCS ${FDBBACKUP_SRCS})
target_link_libraries(fdbbackup PRIVATE fdbclient)

install(TARGETS fdbbackup DESTINATION ${FDB_BIN_DIR} COMPONENT clients)
install(PROGRAMS $<TARGET_FILE:fdbbackup>
  DESTINATION ${FDB_LIB_DIR}/foundationdb/backup_agent
  RENAME backup_agent
  COMPONENT clients)
install(PROGRAMS $<TARGET_FILE:fdbbackup>
  DESTINATION ${FDB_BIN_DIR}
  RENAME fdbrestore
  COMPONENT clients)
install(PROGRAMS $<TARGET_FILE:fdbbackup>
  DESTINATION ${FDB_BIN_DIR}
  RENAME dr_agent
  COMPONENT clients)
install(PROGRAMS $<TARGET_FILE:fdbbackup>
  DESTINATION ${FDB_BIN_DIR}
  RENAME fdbdr
  COMPONENT clients)
