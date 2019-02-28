################################################################################
# Helper Functions
################################################################################

function(install_symlink_impl)
  if (NOT WIN32)
    set(options "")
    set(one_value_options TO DESTINATION)
    set(multi_value_options COMPONENTS)
    cmake_parse_arguments(SYM "${options}" "${one_value_options}" "${multi_value_options}" "${ARGN}")

    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/symlinks)
    get_filename_component(fname ${SYM_DESTINATION} NAME)
    get_filename_component(dest_dir ${SYM_DESTINATION} DIRECTORY)
    set(sl ${CMAKE_CURRENT_BINARY_DIR}/symlinks/${fname})
    execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${SYM_TO} ${sl})
    foreach(component IN LISTS SYM_COMPONENTS)
      install(FILES ${sl} DESTINATION ${dest_dir} COMPONENT ${component})
    endforeach()
  endif()
endfunction()

function(install_symlink)
  if(NOT WIN32 AND NOT OPEN_FOR_IDE)
    set(options "")
    set(one_value_options COMPONENT LINK_DIR FILE_DIR LINK_NAME FILE_NAME)
    set(multi_value_options "")
    cmake_parse_arguments(IN "${options}" "${one_value_options}" "${multi_value_options}" "${ARGN}")

    string(REGEX MATCHALL "\\/" slashes "${IN_LINK_NAME}")
    list(LENGTH slashes num_link_subdirs)
    foreach(i RANGE 1 ${num_link_subdirs})
      set(rel_path "../${rel_path}")
    endforeach()
    if("${IN_FILE_DIR}" MATCHES "bin")
      if("${IN_LINK_DIR}" MATCHES "lib")
        install_symlink_impl(
          TO "../${rel_path}/bin/${IN_FILE_NAME}"
          DESTINATION "lib/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-tgz")
        install_symlink_impl(
          TO "../${rel_path}/bin/${IN_FILE_NAME}"
          DESTINATION "usr/lib64/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-el6"
                     "${IN_COMPONENT}-el7"
                     "${IN_COMPONENT}-deb")
        install_symlink_impl(
          TO "../${rel_path}/bin/${IN_FILE_NAME}"
          DESTINATION "usr/lib64/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-deb")
      elseif("${IN_LINK_DIR}" MATCHES "bin")
        install_symlink_impl(
          TO "../${rel_path}/bin/${IN_FILE_NAME}"
          DESTINATION "bin/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-tgz")
        install_symlink_impl(
          TO "../${rel_path}/bin/${IN_FILE_NAME}"
          DESTINATION "usr/bin/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-el6"
                     "${IN_COMPONENT}-el7"
                     "${IN_COMPONENT}-deb")
      else()
        message(FATAL_ERROR "Unknown LINK_DIR ${IN_LINK_DIR}")
      endif()
    else()
      message(FATAL_ERROR "Unknown FILE_DIR ${IN_FILE_DIR}")
    endif()
  endif()
endfunction()

function(fdb_install)
  if(NOT WIN32 AND NOT OPEN_FOR_IDE)
    set(options EXPORT)
    set(one_value_options COMPONENT DESTINATION)
    set(multi_value_options TARGETS FILES DIRECTORY)
    cmake_parse_arguments(IN "${options}" "${one_value_options}" "${multi_value_options}" "${ARGN}")

    if(IN_TARGETS)
      set(args TARGETS ${IN_TARGETS})
    elseif(IN_FILES)
      set(args FILES ${IN_FILES})
    elseif(IN_DIRECTORY)
      set(args DIRECTORY ${IN_DIRECTORY})
    else()
      message(FATAL_ERROR "Expected FILES or TARGETS")
    endif()
    if(IN_EXPORT)
      set(args EXPORT)
    endif()
    if("${IN_DESTINATION}" MATCHES "bin")
      install(${args} DESTINATION "bin" COMPONENT "${IN_COMPONENT}-tgz")
      install(${args} DESTINATION "usr/bin" COMPONENT "${IN_COMPONENT}-deb")
      install(${args} DESTINATION "usr/bin" COMPONENT "${IN_COMPONENT}-el6")
      install(${args} DESTINATION "usr/bin" COMPONENT "${IN_COMPONENT}-el7")
      install(${args} DESTINATION "usr/local/bin" COMPONENT "${IN_COMPONENT}-pm")
    elseif("${IN_DESTINATION}" MATCHES "sbin")
      install(${args} DESTINATION "sbin" COMPONENT "${IN_COMPONENT}-tgz")
      install(${args} DESTINATION "usr/sbin" COMPONENT "${IN_COMPONENT}-deb")
      install(${args} DESTINATION "usr/sbin" COMPONENT "${IN_COMPONENT}-el6")
      install(${args} DESTINATION "usr/sbin" COMPONENT "${IN_COMPONENT}-el7")
      install(${args} DESTINATION "usr/local/libexec" COMPONENT "${IN_COMPONENT}-pm")
    elseif("${IN_DESTINATION}" MATCHES "libexec")
      install(${args} DESTINATION "libexec" COMPONENT "${IN_COMPONENT}-tgz")
      install(${args} DESTINATION "usr/libexec" COMPONENT "${IN_COMPONENT}-deb")
      install(${args} DESTINATION "usr/libexec" COMPONENT "${IN_COMPONENT}-el6")
      install(${args} DESTINATION "usr/libexec" COMPONENT "${IN_COMPONENT}-el7")
      install(${args} DESTINATION "usr/local/lib/foundationdb" COMPONENT "${IN_COMPONENT}-pm")
    elseif("${IN_DESTINATION}" MATCHES "include")
      install(${args} DESTINATION "include" COMPONENT "${IN_COMPONENT}-tgz")
      install(${args} DESTINATION "usr/include" COMPONENT "${IN_COMPONENT}-deb")
      install(${args} DESTINATION "usr/include" COMPONENT "${IN_COMPONENT}-el6")
      install(${args} DESTINATION "usr/include" COMPONENT "${IN_COMPONENT}-el7")
      install(${args} DESTINATION "usr/local/include" COMPONENT "${IN_COMPONENT}-pm")
    elseif("${IN_DESTINATION}" MATCHES "etc")
      install(${args} DESTINATION "etc/foundationdb" COMPONENT "${IN_COMPONENT}-tgz")
      install(${args} DESTINATION "etc/foundationdb" COMPONENT "${IN_COMPONENT}-deb")
      install(${args} DESTINATION "etc/foundationdb" COMPONENT "${IN_COMPONENT}-el6")
      install(${args} DESTINATION "etc/foundationdb" COMPONENT "${IN_COMPONENT}-el7")
      install(${args} DESTINATION "usr/local/etc/foundationdb" COMPONENT "${IN_COMPONENT}-pm")
    elseif("${IN_DESTINATION}" MATCHES "log")
      install(${args} DESTINATION "log/foundationdb" COMPONENT "${IN_COMPONENT}-tgz")
      install(${args} DESTINATION "var/log/foundationdb" COMPONENT "${IN_COMPONENT}-deb")
      install(${args} DESTINATION "var/log/foundationdb" COMPONENT "${IN_COMPONENT}-el6")
      install(${args} DESTINATION "var/log/foundationdb" COMPONENT "${IN_COMPONENT}-el7")
    elseif("${IN_DESTINATION}" MATCHES "data")
      install(${args} DESTINATION "lib/foundationdb" COMPONENT "${IN_COMPONENT}-tgz")
      install(${args} DESTINATION "var/lib/foundationdb" COMPONENT "${IN_COMPONENT}-deb")
      install(${args} DESTINATION "var/lib/foundationdb" COMPONENT "${IN_COMPONENT}-el6")
      install(${args} DESTINATION "var/lib/foundationdb" COMPONENT "${IN_COMPONENT}-el7")
    endif()
  endif()
endfunction()

if(APPLE)
  set(CPACK_GENERATOR TGZ PackageMaker)
else()
  set(CPACK_GENERATOR RPM DEB TGZ)
endif()


set(CPACK_PACKAGE_CHECKSUM SHA256)
set(CPACK_PROJECT_CONFIG_FILE "${CMAKE_SOURCE_DIR}/cmake/CPackConfig.cmake")

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
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${FDB_VERSION}-${CPACK_SYSTEM_NAME}")
set(CPACK_OUTPUT_FILE_PREFIX "${CMAKE_BINARY_DIR}/packages")
set(CPACK_PACKAGE_DESCRIPTION_FILE ${CMAKE_SOURCE_DIR}/packaging/description)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
  "FoundationDB is a scalable, fault-tolerant, ordered key-value store with full ACID transactions.")
set(CPACK_PACKAGE_ICON ${CMAKE_SOURCE_DIR}/packaging/foundationdb.ico)
set(CPACK_PACKAGE_CONTACT "The FoundationDB Community")
set(CPACK_COMPONENT_server_DEPENDS clients)
if (INSTALL_LAYOUT MATCHES "OSX")
  # MacOS needs a file exiension for the LICENSE file
  set(CPACK_RESOURCE_FILE_README ${CMAKE_SOURCE_DIR}/packaging/osx/resources/conclusion.rtf)
  set(CPACK_PRODUCTBUILD_RESOURCES_DIR ${CMAKE_SOURCE_DIR}/packaging/osx/resources)
  configure_file(${CMAKE_SOURCE_DIR}/LICENSE ${CMAKE_BINARY_DIR}/License.txt COPYONLY)
  set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_BINARY_DIR}/License.txt)
else()
  set(CPACK_RESOURCE_FILE_README ${CMAKE_SOURCE_DIR}/README.md)
  set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_SOURCE_DIR}/LICENSE)
endif()

################################################################################
# Configuration for RPM
################################################################################

file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/packaging/emptydir")
fdb_install(DIRECTORY "${CMAKE_BINARY_DIR}/packaging/emptydir/" DESTINATION log COMPONENT server)
fdb_install(DIRECTORY "${CMAKE_BINARY_DIR}/packaging/emptydir/" DESTINATION lib COMPONENT server)

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
set(CPACK_RPM_DEBUGINFO_PACKAGE ON)
set(CPACK_RPM_BUILD_SOURCE_DIRS_PREFIX /usr/src)
set(CPACK_RPM_COMPONENT_INSTALL ON)

set(CPACK_RPM_clients-el6_PRE_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preclients.sh)
set(CPACK_RPM_clients-el7_PRE_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preclients.sh)

set(CPACK_RPM_clients-el6_POST_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/postclients.sh)
set(CPACK_RPM_clients-el7_POST_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/postclients.sh)

set(CPACK_RPM_server-el6_PRE_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preserver.sh)
set(CPACK_RPM_server-el7_PRE_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preserver.sh)

set(CPACK_RPM_server-el6_POST_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/postserver.sh)
set(CPACK_RPM_server-el7_POST_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/postserver.sh)

set(CPACK_RPM_server-el6_PRE_UNINSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preunserver.sh)
set(CPACK_RPM_server-el7_PRE_UNINSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preunserver.sh)

set(CPACK_RPM_server-el6_PACKAGE_REQUIRES
  "foundationdb-clients = ${FDB_MAJOR}.${FDB_MINOR}.${FDB_PATCH}, initscripts >= 9.03")
set(CPACK_RPM_server-el7_PACKAGE_REQUIRES
  "foundationdb-clients = ${FDB_MAJOR}.${FDB_MINOR}.${FDB_PATCH}")
#set(CPACK_RPM_java_PACKAGE_REQUIRES
#  "foundationdb-clients = ${FDB_MAJOR}.${FDB_MINOR}.${FDB_PATCH}")
set(CPACK_RPM_python_PACKAGE_REQUIRES
  "foundationdb-clients = ${FDB_MAJOR}.${FDB_MINOR}.${FDB_PATCH}")

################################################################################
# Configuration for DEB
################################################################################

set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_DEBUGINFO_PACKAGE ON)
set(CPACK_DEBIAN_PACKAGE_SECTION "database")
set(CPACK_DEBIAN_ENABLE_COMPONENT_DEPENDS ON)

set(CPACK_DEBIAN_SERVER-DEB_PACKAGE_DEPENDS "adduser, libc6 (>= 2.12), python (>= 2.6), foundationdb-clients (= ${FDB_VERSION})")
set(CPACK_DEBIAN_CLIENTS-DEB_PACKAGE_DEPENDS "adduser, libc6 (>= 2.12)")
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "https://www.foundationdb.org")
set(CPACK_DEBIAN_CLIENTS-DEB_PACKAGE_CONTROL_EXTRA
  ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-clients/postinst)
set(CPACK_DEBIAN_SERVER-DEB_PACKAGE_CONTROL_EXTRA
  ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/conffiles
  ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/preinst
  ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/postinst
  ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/prerm
  ${CMAKE_SOURCE_DIR}/packaging/deb/DEBIAN-foundationdb-server/postrm)

################################################################################
# MacOS configuration
################################################################################

if(NOT WIN32)
  install(PROGRAMS ${CMAKE_SOURCE_DIR}/packaging/osx/uninstall-FoundationDB.sh
    DESTINATION "usr/local/foundationdb"
    COMPONENT clients-pm)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/osx/com.foundationdb.fdbmonitor.plist
    DESTINATION "Library/LaunchDaemons"
    COMPONENT server-pm)
endif()

################################################################################
# Server configuration
################################################################################

string(RANDOM LENGTH 8 description1)
string(RANDOM LENGTH 8 description2)
set(CLUSTER_DESCRIPTION1 ${description1} CACHE STRING "Cluster description")
set(CLUSTER_DESCRIPTION2 ${description2} CACHE STRING "Cluster description")

if(NOT WIN32)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/osx/foundationdb.conf.new
    DESTINATION "usr/local/etc"
    COMPONENT server-pm)
  fdb_install(FILES ${CMAKE_SOURCE_DIR}/packaging/foundationdb.conf
    DESTINATION etc
    COMPONENT server)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/argparse.py
    DESTINATION "usr/lib/foundationdb"
    COMPONENT server-el6)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/make_public.py
    DESTINATION "usr/lib/foundationdb"
    COMPONENT server-el6)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/argparse.py
    DESTINATION "usr/lib/foundationdb"
    COMPONENT server-deb)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/make_public.py
    DESTINATION "usr/lib/foundationdb"
    COMPONENT server-deb)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/rpm/foundationdb.service
    DESTINATION "lib/systemd/system"
    COMPONENT server-el7)
  install(PROGRAMS ${CMAKE_SOURCE_DIR}/packaging/rpm/foundationdb-init
    DESTINATION "etc/rc.d/init.d"
    RENAME "foundationdb"
    COMPONENT server-el6)
  install(PROGRAMS ${CMAKE_SOURCE_DIR}/packaging/deb/foundationdb-init
    DESTINATION "etc/init.d"
    RENAME "foundationdb"
    COMPONENT server-deb)
endif()
