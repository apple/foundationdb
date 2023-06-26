set(xp_tarball_name ${CMAKE_BINARY_DIR}/packages/frostdb-${CMAKE_SYSTEM_PROCESSOR}-${FDB_VERSION}-snapshot.tar.gz)

add_custom_command(OUTPUT ${xp_tarball_name}
  COMMAND ${CMAKE_COMMAND} -E env PYTHONPATH="${CMAKE_SOURCE_DIR}/tests/TestRunner:${CMAKE_BINARY_DIR}/tests/TestRunner"
          $<TARGET_FILE:Python3::Interpreter> ${CMAKE_CURRENT_SOURCE_DIR}/build-xp-tarball.py
          --output ${xp_tarball_name}
          --build-dir ${CMAKE_BINARY_DIR}
          --src-dir ${CMAKE_SOURCE_DIR}
          --external-clients ${FDB_PREV_RELEASE_VERSION}
          --release-version ${FDB_VERSION}
  DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/build-xp-tarball.py
    $<TARGET_FILE:fdb_c>
    $<TARGET_FILE:fdbserver>
    $<TARGET_FILE:fdbcli>
    $<TARGET_FILE:fdbserver>
  COMMENT "Creating FDB dependency tarball for XP")

add_custom_target(package_xp_tarball DEPENDS ${xp_tarball_name})