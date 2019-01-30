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
  set(FDB_LIB_NOSUFFIX "lib")
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
    set(FDB_LIB_NOSUFFIX "usr/lib")
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
    set(FDB_LIB_NOSUFFIX "usr/lib")
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
set(CPACK_PACKAGE_VENDOR "FoundationDB <fdb-dist@apple.com>")
set(CPACK_PACKAGE_VERSION_MAJOR ${FDB_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${FDB_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${FDB_PATCH})
set(CPACK_PACKAGE_DESCRIPTION_FILE ${CMAKE_SOURCE_DIR}/packaging/description)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
  "FoundationDB is a scalable, fault-tolerant, ordered key-value store with full ACID transactions.")
set(CPACK_PACKAGE_ICON ${CMAKE_SOURCE_DIR}/packaging/foundationdb.ico)
set(CPACK_PACKAGE_CONTACT "The FoundationDB Community")
set(CPACK_COMPONENT_server_DEPENDS clients)
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
    "%attr(0700,foundationdb,foundationdb) /var/log/foundationdb"
    "%attr(0700, foundationdb, foundationdb) /var/lib/foundationdb")
  set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
    "/usr/sbin"
    "/usr/share/java"
    "/usr/lib64/python2.7"
    "/usr/lib64/python2.7/site-packages"
    "/var"
    "/var/log"
    "/var/lib"
    "/lib"
    "/lib/systemd"
    "/lib/systemd/system"
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
endif()

################################################################################
# Configuration for DEB
################################################################################

if(INSTALL_LAYOUT MATCHES "DEB")
  set(CPACK_DEB_COMPONENT_INSTALL ON)
  set(CPACK_DEBIAN_PACKAGE_SECTION "database")
  set(CPACK_DEBIAN_ENABLE_COMPONENT_DEPENDS ON)

  set(CPACK_DEBIAN_server_PACKAGE_DEPENDS "adduser, libc6 (>= 2.11), python (>= 2.6)")
  set(CPACK_DEBIAN_clients_PACKAGE_DEPENDS "adduser, libc6 (>= 2.11)")
  set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "https://www.foundationdb.org")
  set(CPACK_DEBIAN_clients_PACKAGE_CONTROL_EXTRA
    ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-clients/postinst)
  set(CPACK_DEBIAN_server_PACKAGE_CONTROL_EXTRA
    ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/conffiles
    ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/preinst
    ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/postinst
    ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/prerm
    ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/postrm)
endif()

################################################################################
# Server configuration
################################################################################

string(RANDOM LENGTH 8 description1)
string(RANDOM LENGTH 8 description2)
set(CLUSTER_DESCRIPTION1 ${description1} CACHE STRING "Cluster description")
set(CLUSTER_DESCRIPTION2 ${description2} CACHE STRING "Cluster description")

install(FILES ${CMAKE_SOURCE_DIR}/packaging/foundationdb.conf
  DESTINATION ${FDB_CONFIG_DIR}
  COMPONENT server)
install(FILES ${CMAKE_SOURCE_DIR}/packaging/argparse.py
  DESTINATION "usr/lib/foundationdb"
  COMPONENT server)
install(FILES ${CMAKE_SOURCE_DIR}/packaging/make_public.py
  DESTINATION "usr/lib/foundationdb")
if((INSTALL_LAYOUT MATCHES "RPM") OR (INSTALL_LAYOUT MATCHES "DEB"))
  file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/packaging/foundationdb
    ${CMAKE_BINARY_DIR}/packaging/rpm)
  install(
    DIRECTORY ${CMAKE_BINARY_DIR}/packaging/foundationdb
    DESTINATION "var/log"
    COMPONENT server)
  install(
    DIRECTORY ${CMAKE_BINARY_DIR}/packaging/foundationdb
    DESTINATION "var/lib"
    COMPONENT server)
  execute_process(
    COMMAND pidof systemd
    RESULT_VARIABLE IS_SYSTEMD
    OUTPUT_QUIET
    ERROR_QUIET)
  if(IS_SYSTEMD EQUAL "0")
    configure_file(${CMAKE_SOURCE_DIR}/packaging/rpm/foundationdb.service
      ${CMAKE_BINARY_DIR}/packaging/rpm/foundationdb.service)
    install(FILES ${CMAKE_BINARY_DIR}/packaging/rpm/foundationdb.service
      DESTINATION "lib/systemd/system"
      COMPONENT server)
  else()
    if(INSTALL_LAYOUT MATCHES "RPM")
      install(FILES ${CMAKE_SOURCE_DIR}/packaging/rpm/foundationdb-init
        DESTINATION "etc/rc.d/init.d"
        RENAME "foundationdb"
        COMPONENT server)
    else()
      install(FILES ${CMAKE_SOURCE_DIR}/packaging/deb/foundationdb-init
        DESTINATION "etc/init.d"
        RENAME "foundationdb"
        COMPONENT server)
    endif()
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
