if(NOT INSTALL_LAYOUT)
  set(DEFAULT_INSTALL_LAYOUT "STANDALONE")
endif()
set(INSTALL_LAYOUT "${DEFAULT_INSTALL_LAYOUT}"
  CACHE STRING "Installation directory layout. Options are: TARGZ (as in tar.gz installer), WIN, STANDALONE, RPM, DEB, OSX")

set(DIR_LAYOUT ${INSTALL_LAYOUT})
if(DIR_LAYOUT MATCHES "TARGZ")
  set(DIR_LAYOUT "STANDALONE")
endif()

if(UNIX)
  get_property(LIB64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
  set(FDB_CONFIG_DIR "etc/foundationdb")
  if("${LIB64}" STREQUAL "TRUE")
    set(LIBSUFFIX 64)
  else()
    set(LIBSUFFIX "")
  endif()
  if(DIR_LAYOUT MATCHES "STANDALONE")
    set(FDB_LIB_DIR "lib${LIBSUFFIX}")
    set(FDB_LIBEXEC_DIR "${FDB_LIB_DIR}")
    set(FDB_BIN_DIR "bin")
    set(FDB_SBIN_DIR "sbin")
    set(FDB_INCLUDE_INSTALL_DIR "include")
    set(FDB_PYTHON_INSTALL_DIR "${FDB_LIB_DIR}/python2.7/site-packages/fdb")
    set(FDB_SHARE_DIR "share")
  elseif(DIR_LAYOUT MATCHES "OSX")
    set(CPACK_GENERATOR productbuild)
    set(CPACK_PACKAGING_INSTALL_PREFIX "/")
    set(FDB_LIB_DIR "usr/local/lib")
    set(FDB_LIBEXEC_DIR "usr/local/libexec")
    set(FDB_BIN_DIR "usr/local/bin")
    set(FDB_SBIN_DIR "usr/local/sbin")
    set(FDB_INCLUDE_INSTALL_DIR "usr/local/include")
    set(FDB_PYTHON_INSTALL_DIR "Library/Python/2.7/site-packages/fdb")
    set(FDB_SHARE_DIR "usr/local/share")
  elseif(DIR_LAYOUT MATCHES "WIN")
    # TODO
  else()
    # for deb and rpm
    if(INSTALL_LAYOUT MATCHES "RPM")
      set(CPACK_GENERATOR "RPM")
    else()
      # DEB
      set(CPACK_GENERATOR "DEB")
    endif()
    set(CMAKE_INSTALL_PREFIX "/")
    set(CPACK_PACKAGING_INSTALL_PREFIX "/")
    set(FDB_LIB_DIR "usr/lib${LIBSUFFIX}")
    set(FDB_LIBEXEC_DIR "${FDB_LIB_DIR}")
    set(FDB_BIN_DIR "usr/bin")
    set(FDB_SBIN_DIR "usr/sbin")
    set(FDB_INCLUDE_INSTALL_DIR "usr/include")
    set(FDB_PYTHON_INSTALL_DIR "${FDB_LIB_DIR}/python2.7/site-packages/fdb")
    set(FDB_SHARE_DIR "usr/share")
  endif()
endif()

################################################################################
# Version information
################################################################################

string(REPLACE "." ";" FDB_VERSION_LIST ${FDB_VERSION_PLAIN})
list(GET FDB_VERSION_LIST 0 FDB_MAJOR)
list(GET FDB_VERSION_LIST 1 FDB_MINOR)
list(GET FDB_VERSION_LIST 2 FDB_PATCH)

################################################################################
# General CPack configuration
################################################################################

include(InstallRequiredSystemLibraries)
set(CPACK_PACKAGE_NAME "foundationdb")
set(CPACK_PACKAGE_VENDOR "FoundationDB Community")
set(CPACK_PACKAGE_VERSION_MAJOR ${FDB_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${FDB_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${FDB_PATCH})
set(CPACK_PACKAGE_DESCRIPTION_FILE ${CMAKE_SOURCE_DIR}/packaging/description)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
  "FoundationDB is a scalable, fault-tolerant, ordered key-value store with full ACID transactions.")
set(CPACK_PACKAGE_ICON ${CMAKE_SOURCE_DIR}/packaging/foundationdb.ico)
if (INSTALL_LAYOUT MATCHES "OSX")
  set(CPACK_RESOURCE_FILE_README ${CMAKE_SOURCE_DIR}/packaging/osx/resources/conclusion.rtf)
  set(CPACK_PRODUCTBUILD_RESOURCES_DIR ${CMAKE_SOURCE_DIR}/packaging/osx/resources)
else()
  set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_SOURCE_DIR}/LICENSE)
  set(CPACK_RESOURCE_FILE_README ${CMAKE_SOURCE_DIR}/README.md)
endif()

################################################################################
# Configuration for RPM
################################################################################

if(INSTALL_LAYOUT MATCHES "RPM")
  set(CPACK_RPM_server_USER_FILELIST
    "%config(noreplace) /etc/foundationdb/foundationdb.conf"
    "%attr(600, root, root) %config(noreplace) /etc/foundationdb/fdb.cluster")
  set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
    "/usr/sbin" "/usr/share/java" "/usr/lib64/python2.7"
    "/usr/lib64/python2.7/site-packages"
    "/lib/systemd"
    "/etc/rc.d/init.d")
  set(CPACK_RPM_DEBUGINFO_PACKAGE ON)
  set(CPACK_RPM_BUILD_SOURCE_DIRS_PREFIX /usr/src)
  set(CPACK_RPM_COMPONENT_INSTALL ON)
  set(CPACK_RPM_clients_PRE_INSTALL_SCRIPT_FILE
    ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preclients.sh)
  set(CPACK_RPM_clients_POST_INSTALL_SCRIPT_FILE
    ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/postclients.sh)
  set(CPACK_RPM_server_PRE_INSTALL_SCRIPT_FILE
    ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preserver.sh)
  set(CPACK_RPM_server_POST_INSTALL_SCRIPT_FILE
    ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/postserver.sh)
  set(CPACK_RPM_server_PRE_UNINSTALL_SCRIPT_FILE
    ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preunserver.sh)
  set(CPACK_RPM_server_PACKAGE_REQUIRES
    "foundationdb-clients = ${FDB_MAJOR}.${FDB_MINOR}.${FDB_PATCH}")
elseif(INSTALL_LAYOUT MATCHES "OSX")
endif()

################################################################################
# Server configuration
################################################################################

string(RANDOM LENGTH 8 description1)
string(RANDOM LENGTH 8 description2)
set(CLUSTER_DESCRIPTION1 ${description1} CACHE STRING "Cluster description")
set(CLUSTER_DESCRIPTION2 ${description2} CACHE STRING "Cluster description")

configure_file(fdb.cluster.cmake ${CMAKE_CURRENT_BINARY_DIR}/fdb.cluster)
install(FILES ${CMAKE_SOURCE_DIR}/packaging/foundationdb.conf
  DESTINATION ${FDB_CONFIG_DIR}
  COMPONENT server)
install(FILES ${CMAKE_BINARY_DIR}/fdb.cluster
  DESTINATION ${FDB_CONFIG_DIR}
  COMPONENT server)
if(INSTALL_LAYOUT MATCHES "RPM")
  execute_process(
    COMMAND pidof systemd
    RESULT_VARIABLE IS_SYSTEMD
    OUTPUT_QUIET
    ERROR_QUIET)
  if(IS_SYSTEMD EQUAL "0")
    install(FILES ${CMAKE_SOURCE_DIR}/packaging/rpm/foundationdb-init
      DESTINATION "lib/systemd/system"
      RENAME "foundationdb.service"
      COMPONENT server)
  else()
    install(FILES ${CMAKE_SOURCE_DIR}/packaging/argparse.py
      DESTINATION "usr/lib/foundationdb"
      COMPONENT server)
    install(FILES ${CMAKE_SOURCE_DIR}/packaging/rpm/foundationdb-init
      DESTINATION "etc/rc.d/init.d"
      RENAME "foundationdb"
      COMPONENT server)
  endif()
endif()

################################################################################
# Helper Macros
################################################################################

macro(install_symlink filepath sympath compondent)
  install(CODE "execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${filepath} ${sympath})" COMPONENT ${component})
  install(CODE "message(\"-- Created symlink: ${sympath} -> ${filepath}\")")
endmacro()
macro(install_mkdir dirname component)
  install(CODE "execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory ${dirname})" COMPONENT ${component})
  install(CODE "message(\"-- Created directory: ${dirname}\")")
endmacro()
