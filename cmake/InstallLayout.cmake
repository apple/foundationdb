if(NOT INSTALL_LAYOUT)
  set(DEFAULT_INSTALL_LAYOUT "STANDALONE")
endif()
set(INSTALL_LAYOUT "${DEFAULT_INSTALL_LAYOUT}"
  CACHE STRING "Installation directory layout. Options are: TARGZ (as in tar.gz installer), WIN (as in zip installer), STANDALONE, RPM, DEB, OSX")

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
  else()
    # for deb and rpm
    set(CMAKE_INSTALL_PREFIX "/")
    set(CPACK_PACKAGING_INSTALL_PREFIX "/")
    set(FDB_LIB_DIR "usr/lib${LIBSUFFIX}")
    set(FDB_LIBEXEC_DIR "${FDB_LIB_DIR}")
    set(FDB_BIN_DIR "usr/bin")
    set(FDB_INCLUDE_INSTALL_DIR "usr/include")
    set(FDB_PYTHON_INSTALL_DIR "${FDB_LIB_DIR}/python2.7/site-packages/fdb")
    set(FDB_SHARE_DIR "usr/share")
  endif()
endif()

include(InstallRequiredSystemLibraries)

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

# configuration for rpm
if(INSTALL_LAYOUT MATCHES "RPM")
  set(CPACK_RPM_server_USER_FILELIST "%config(noreplace) /etc/foundationdb/foundationdb.conf")
  set(CPACK_RPM_DEBUGINFO_PACKAGE ON)
  set(CPACK_RPM_BUILD_SOURCE_DIRS_PREFIX /usr/src)
  set(CPACK_RPM_COMPONENT_INSTALL ON)
  #set(CPACK_RPM_clients_PRE_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/packaging/rpm/preinstall-client.sh)
  #set(CPACK_RPM_server_PRE_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/packaging/rpm/preinstall-server.sh)
  #set(CPACK_RPM_server_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/packaging/rpm/postinstall-server.sh)

  #set(CPACK_RPM_server_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/packaging/rpm/preuninstall-server.sh)
elseif(INSTALL_LAYOUT MATCHES "OSX")
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
