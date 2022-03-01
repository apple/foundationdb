include(FDBInstall)

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

    set(rel_path "")
    string(REGEX MATCHALL "\\/" slashes "${IN_LINK_NAME}")
    foreach(ignored IN LISTS slashes)
      set(rel_path "../${rel_path}")
    endforeach()
    if("${IN_FILE_DIR}" MATCHES "bin")
      if("${IN_LINK_DIR}" MATCHES "lib")
        install_symlink_impl(
          TO "../${rel_path}bin/${IN_FILE_NAME}"
          DESTINATION "lib/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-tgz")
        install_symlink_impl(
          TO "../${rel_path}bin/${IN_FILE_NAME}"
          DESTINATION "usr/lib64/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-el6"
                     "${IN_COMPONENT}-el7"
                     "${IN_COMPONENT}-deb")
        install_symlink_impl(
          TO "../${rel_path}bin/${IN_FILE_NAME}"
          DESTINATION "usr/lib64/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-deb")
      elseif("${IN_LINK_DIR}" MATCHES "bin")
        install_symlink_impl(
          TO "../${rel_path}bin/${IN_FILE_NAME}"
          DESTINATION "bin/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-tgz")
        install_symlink_impl(
          TO "../${rel_path}bin/${IN_FILE_NAME}"
          DESTINATION "usr/bin/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-el6"
                     "${IN_COMPONENT}-el7"
                     "${IN_COMPONENT}-deb")
      elseif("${IN_LINK_DIR}" MATCHES "fdbmonitor")
        install_symlink_impl(
          TO "../../${rel_path}bin/${IN_FILE_NAME}"
          DESTINATION "lib/foundationdb/${IN_LINK_NAME}"
          COMPONENTS "${IN_COMPONENT}-tgz")
        install_symlink_impl(
          TO "../../${rel_path}bin/${IN_FILE_NAME}"
          DESTINATION "usr/lib/foundationdb/${IN_LINK_NAME}"
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

function(symlink_files)
  if (NOT WIN32)
    set(options "")
    set(one_value_options LOCATION SOURCE)
    set(multi_value_options TARGETS)
    cmake_parse_arguments(SYM "${options}" "${one_value_options}" "${multi_value_options}" "${ARGN}")

    file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${SYM_LOCATION})
    foreach(component IN LISTS SYM_TARGETS)
      execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${SYM_SOURCE} ${CMAKE_BINARY_DIR}/${SYM_LOCATION}/${component} WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/${SYM_LOCATION})
    endforeach()
  endif()
endfunction()

fdb_install_packages(TGZ DEB EL7 VERSIONED)
fdb_install_dirs(BIN SBIN LIB FDBMONITOR INCLUDE ETC LOG DATA BACKUPAGENT)
message(STATUS "FDB_INSTALL_DIRS -> ${FDB_INSTALL_DIRS}")

install_destinations(TGZ
  BIN bin
  SBIN sbin
  LIB lib
  FDBMONITOR sbin
  BACKUPAGENT usr/lib/foundationdb
  INCLUDE include
  ETC etc/foundationdb
  LOG log/foundationdb
  DATA lib/foundationdb)
copy_install_destinations(TGZ VERSIONED PREFIX "usr/lib/foundationdb-${FDB_VERSION}/")
install_destinations(DEB
  BIN usr/bin
  SBIN usr/sbin
  LIB usr/lib
  FDBMONITOR usr/lib/foundationdb
  BACKUPAGENT usr/lib/foundationdb
  INCLUDE usr/include
  ETC etc/foundationdb
  LOG var/log/foundationdb
  DATA var/lib/foundationdb/data)
copy_install_destinations(DEB EL7)
install_destinations(EL7 LIB usr/lib64)

# This can be used for debugging in case above is behaving funky
#print_install_destinations()

set(generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")

if(APPLE)
  set(CPACK_GENERATOR TGZ)
else()
  set(CPACK_GENERATOR RPM DEB TGZ)
endif()


set(CPACK_PACKAGE_CHECKSUM SHA256)
configure_file("${CMAKE_SOURCE_DIR}/cmake/CPackConfig.cmake" "${CMAKE_BINARY_DIR}/packaging/CPackConfig.cmake")
set(CPACK_PROJECT_CONFIG_FILE "${CMAKE_BINARY_DIR}/packaging/CPackConfig.cmake")

################################################################################
# User config
################################################################################

set(GENERATE_DEBUG_PACKAGES "${FDB_RELEASE}" CACHE BOOL "Build debug rpm/deb packages (default: only ON for FDB_RELEASE)")

################################################################################
# Alternatives config
################################################################################

set(mv_packaging_dir ${PROJECT_SOURCE_DIR}/packaging/multiversion)
# USE PROJECT_VERSION_* HERE AS THEY ARE GUARANTEED TO BE INTEGERS ONLY
math(EXPR ALTERNATIVES_PRIORITY "(${PROJECT_VERSION_MAJOR} * 1000) + (${PROJECT_VERSION_MINOR} * 100) + ${PROJECT_VERSION_PATCH}")
set(script_dir "${PROJECT_BINARY_DIR}/packaging/multiversion/")
file(MAKE_DIRECTORY "${script_dir}/server" "${script_dir}/clients")

# Needs to to be named postinst for debian
configure_file("${mv_packaging_dir}/server/postinst-deb" "${script_dir}/server/postinst" @ONLY)

configure_file("${mv_packaging_dir}/server/postinst-rpm" "${script_dir}/server" @ONLY)
configure_file("${mv_packaging_dir}/server/prerm" "${script_dir}/server" @ONLY)
set(LIB_DIR lib)
configure_file("${mv_packaging_dir}/clients/postinst" "${script_dir}/clients" @ONLY)
set(LIB_DIR lib64)
configure_file("${mv_packaging_dir}/clients/postinst" "${script_dir}/clients/postinst-el7" @ONLY)
configure_file("${mv_packaging_dir}/clients/prerm" "${script_dir}/clients" @ONLY)

#make sure all directories we need exist
file(MAKE_DIRECTORY "${script_dir}/clients/usr/lib/foundationdb")
install(DIRECTORY "${script_dir}/clients/usr/lib/foundationdb"
  DESTINATION usr/lib
  COMPONENT clients-versioned)
file(MAKE_DIRECTORY "${script_dir}/clients/usr/lib/pkgconfig")
install(DIRECTORY "${script_dir}/clients/usr/lib/pkgconfig"
  DESTINATION usr/lib
  COMPONENT clients-versioned)
file(MAKE_DIRECTORY "${script_dir}/clients/usr/lib/cmake")
install(DIRECTORY "${script_dir}/clients/usr/lib/cmake"
  DESTINATION usr/lib
  COMPONENT clients-versioned)

################################################################################
# Move Docker Setup
################################################################################

file(COPY "${PROJECT_SOURCE_DIR}/packaging/docker" DESTINATION "${PROJECT_BINARY_DIR}/packages/")

################################################################################
# General CPack configuration
################################################################################

include(InstallRequiredSystemLibraries)
set(CPACK_PACKAGE_NAME "foundationdb")
set(CPACK_PACKAGE_VENDOR "FoundationDB")
set(CPACK_PACKAGE_CONTACT "fdb-dist@apple.com")
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

set(CPACK_COMPONENT_SERVER-EL7_DEPENDS clients-el7)
set(CPACK_COMPONENT_SERVER-DEB_DEPENDS clients-deb)
set(CPACK_COMPONENT_SERVER-TGZ_DEPENDS clients-tgz)
set(CPACK_COMPONENT_SERVER-VERSIONED_DEPENDS clients-versioned)
set(CPACK_RPM_SERVER-VERSIONED_PACKAGE_REQUIRES
  "foundationdb${FDB_VERSION}-clients")

set(CPACK_COMPONENT_SERVER-EL7_DISPLAY_NAME "foundationdb-server")
set(CPACK_COMPONENT_SERVER-DEB_DISPLAY_NAME "foundationdb-server")
set(CPACK_COMPONENT_SERVER-TGZ_DISPLAY_NAME "foundationdb-server")
set(CPACK_COMPONENT_SERVER-VERSIONED_DISPLAY_NAME "foundationdb${FDB_VERSION}-server")

set(CPACK_COMPONENT_CLIENTS-EL7_DISPLAY_NAME "foundationdb-clients")
set(CPACK_COMPONENT_CLIENTS-DEB_DISPLAY_NAME "foundationdb-clients")
set(CPACK_COMPONENT_CLIENTS-TGZ_DISPLAY_NAME "foundationdb-clients")
set(CPACK_COMPONENT_CLIENTS-VERSIONED_DISPLAY_NAME "foundationdb${FDB_VERSION}-clients")


# MacOS needs a file extension for the LICENSE file
configure_file(${CMAKE_SOURCE_DIR}/LICENSE ${CMAKE_BINARY_DIR}/License.txt COPYONLY)

################################################################################
# Filename of packages
################################################################################

if(NOT FDB_RELEASE)
  if(CURRENT_GIT_VERSION)
    set(git_string ".${CURRENT_GIT_VERSION}")
  endif()
  set(CPACK_RPM_PACKAGE_RELEASE 0)
  set(not_fdb_release_string "-0${git_string}.SNAPSHOT")
else()
  set(CPACK_RPM_PACKAGE_RELEASE 1)
  set(not_fdb_release_string "-1")
endif()

#############
# Filenames #
#############
set(unversioned_postfix "${FDB_VERSION}${not_fdb_release_string}")
# RPM filenames
set(rpm-clients-filename "foundationdb-clients-${unversioned_postfix}")
set(rpm-server-filename "foundationdb-server-${unversioned_postfix}")
set(rpm-clients-versioned-filename "foundationdb${FDB_VERSION}-clients${prerelease_string}")
set(rpm-server-versioned-filename "foundationdb${FDB_VERSION}-server${prerelease_string}")

# Deb filenames
set(deb-clients-filename "foundationdb-clients_${unversioned_postfix}")
set(deb-server-filename "foundationdb-server_${unversioned_postfix}")
set(deb-clients-versioned-filename "foundationdb${FDB_VERSION}-clients${prerelease_string}")
set(deb-server-versioned-filename "foundationdb${FDB_VERSION}-server${prerelease_string}")

################################################################################
# Configuration for RPM
################################################################################

set(CPACK_RPM_PACKAGE_LICENSE "Apache 2.0")

set(CPACK_RPM_PACKAGE_NAME "foundationdb")
set(CPACK_RPM_CLIENTS-EL7_PACKAGE_NAME "foundationdb-clients")
set(CPACK_RPM_SERVER-EL7_PACKAGE_NAME "foundationdb-server")
set(CPACK_RPM_SERVER-VERSIONED_PACKAGE_NAME "foundationdb${FDB_VERSION}-server")
set(CPACK_RPM_CLIENTS-VERSIONED_PACKAGE_NAME "foundationdb${FDB_VERSION}-clients")

set(CPACK_RPM_CLIENTS-EL7_FILE_NAME "${rpm-clients-filename}.el7.${CMAKE_SYSTEM_PROCESSOR}.rpm")
set(CPACK_RPM_CLIENTS-VERSIONED_FILE_NAME "${rpm-clients-versioned-filename}.versioned.${CMAKE_SYSTEM_PROCESSOR}.rpm")
set(CPACK_RPM_SERVER-EL7_FILE_NAME "${rpm-server-filename}.el7.${CMAKE_SYSTEM_PROCESSOR}.rpm")
set(CPACK_RPM_SERVER-VERSIONED_FILE_NAME "${rpm-server-versioned-filename}.versioned.${CMAKE_SYSTEM_PROCESSOR}.rpm")

set(CPACK_RPM_CLIENTS-EL7_DEBUGINFO_FILE_NAME "${rpm-clients-filename}.el7-debuginfo.${CMAKE_SYSTEM_PROCESSOR}.rpm")
set(CPACK_RPM_CLIENTS-VERSIONED_DEBUGINFO_FILE_NAME "${rpm-clients-versioned-filename}.versioned-debuginfo.${CMAKE_SYSTEM_PROCESSOR}.rpm")
set(CPACK_RPM_SERVER-EL7_DEBUGINFO_FILE_NAME "${rpm-server-filename}.el7-debuginfo.${CMAKE_SYSTEM_PROCESSOR}.rpm")
set(CPACK_RPM_SERVER-VERSIONED_DEBUGINFO_FILE_NAME "${rpm-server-versioned-filename}.versioned-debuginfo.${CMAKE_SYSTEM_PROCESSOR}.rpm")

file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/packaging/emptydir")
fdb_install(DIRECTORY "${CMAKE_BINARY_DIR}/packaging/emptydir/" DESTINATION data COMPONENT server)
fdb_install(DIRECTORY "${CMAKE_BINARY_DIR}/packaging/emptydir/" DESTINATION log COMPONENT server)
fdb_install(DIRECTORY "${CMAKE_BINARY_DIR}/packaging/emptydir/" DESTINATION etc COMPONENT clients)

set(CPACK_RPM_SERVER-EL7_USER_FILELIST
  "%config(noreplace) /etc/foundationdb/foundationdb.conf"
  "%attr(0700,foundationdb,foundationdb) /var/log/foundationdb"
  "%attr(0700, foundationdb, foundationdb) /var/lib/foundationdb")
set(CPACK_RPM_CLIENTS-EL7_USER_FILELIST "%dir /etc/foundationdb")
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
  "/usr/sbin"
  "/usr/share/java"
  "/usr/lib"
  "/usr/lib64/cmake"
  "/etc/foundationdb"
  "/usr/lib64/pkgconfig"
  "/usr/lib64/python2.7"
  "/usr/lib64/python2.7/site-packages"
  "/var"
  "/var/log"
  "/var/lib"
  "/lib"
  "/lib/systemd"
  "/lib/systemd/system"
  "/etc/rc.d/init.d"
  "/usr/lib/pkgconfig"
  "/usr/lib/foundationdb"
  "/usr/lib/cmake"
  "/usr/lib/foundationdb-${FDB_VERSION}/etc/foundationdb"
  )
set(CPACK_RPM_DEBUGINFO_PACKAGE ${GENERATE_DEBUG_PACKAGES})
#set(CPACK_RPM_BUILD_SOURCE_FDB_INSTALL_DIRS_PREFIX /usr/src)
set(CPACK_RPM_COMPONENT_INSTALL ON)

set(CPACK_RPM_clients-el7_PRE_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preclients.sh)

set(CPACK_RPM_CLIENTS-EL7_POST_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/postclients.sh)

set(CPACK_RPM_SERVER-EL7_PRE_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preserver.sh)

set(CPACK_RPM_SERVER-EL7_POST_INSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/postserver.sh)

set(CPACK_RPM_SERVER-EL7_PRE_UNINSTALL_SCRIPT_FILE
  ${CMAKE_SOURCE_DIR}/packaging/rpm/scripts/preunserver.sh)

set(CPACK_RPM_SERVER-EL7_PACKAGE_REQUIRES
  "foundationdb-clients = ${FDB_MAJOR}.${FDB_MINOR}.${FDB_PATCH}")

set(CPACK_RPM_SERVER-VERSIONED_POST_INSTALL_SCRIPT_FILE
  ${CMAKE_BINARY_DIR}/packaging/multiversion/server/postinst-rpm)

set(CPACK_RPM_SERVER-VERSIONED_PRE_UNINSTALL_SCRIPT_FILE
  ${CMAKE_BINARY_DIR}/packaging/multiversion/server/prerm)

set(CPACK_RPM_CLIENTS-VERSIONED_POST_INSTALL_SCRIPT_FILE
  ${CMAKE_BINARY_DIR}/packaging/multiversion/clients/postinst-el7)

set(CPACK_RPM_CLIENTS-VERSIONED_PRE_UNINSTALL_SCRIPT_FILE
  ${CMAKE_BINARY_DIR}/packaging/multiversion/clients/prerm)

################################################################################
# Configuration for DEB
################################################################################

if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
  set(CPACK_DEBIAN_CLIENTS-DEB_FILE_NAME "${deb-clients-filename}_amd64.deb")
  set(CPACK_DEBIAN_SERVER-DEB_FILE_NAME "${deb-server-filename}_amd64.deb")
  set(CPACK_DEBIAN_CLIENTS-VERSIONED_FILE_NAME "${deb-clients-versioned-filename}.versioned_amd64.deb")
  set(CPACK_DEBIAN_SERVER-VERSIONED_FILE_NAME "${deb-server-versioned-filename}.versioned_amd64.deb")
else()
  set(CPACK_DEBIAN_CLIENTS-DEB_FILE_NAME "${deb-clients-filename}_${CMAKE_SYSTEM_PROCESSOR}.deb")
  set(CPACK_DEBIAN_SERVER-DEB_FILE_NAME "${deb-server-filename}_${CMAKE_SYSTEM_PROCESSOR}.deb")
  set(CPACK_DEBIAN_CLIENTS-VERSIONED_FILE_NAME "${deb-clients-versioned-filename}.versioned_${CMAKE_SYSTEM_PROCESSOR}.deb")
  set(CPACK_DEBIAN_SERVER-VERSIONED_FILE_NAME "${deb-server-versioned-filename}.versioned_${CMAKE_SYSTEM_PROCESSOR}.deb")
endif()

set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_DEBUGINFO_PACKAGE ${GENERATE_DEBUG_PACKAGES})
set(CPACK_DEBIAN_PACKAGE_SECTION "database")
set(CPACK_DEBIAN_ENABLE_COMPONENT_DEPENDS ON)

set(CPACK_DEBIAN_SERVER-DEB_PACKAGE_NAME "foundationdb-server")
set(CPACK_DEBIAN_CLIENTS-DEB_PACKAGE_NAME "foundationdb-clients")
set(CPACK_DEBIAN_SERVER-VERSIONED_PACKAGE_NAME "foundationdb${FDB_VERSION}-server")
set(CPACK_DEBIAN_CLIENTS-VERSIONED_PACKAGE_NAME "foundationdb${FDB_VERSION}-clients")

set(CPACK_DEBIAN_SERVER-DEB_PACKAGE_DEPENDS "adduser, libc6 (>= 2.12), foundationdb-clients (= ${FDB_VERSION})")
set(CPACK_DEBIAN_SERVER-DEB_PACKAGE_RECOMMENDS "python (>= 2.6)")
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

set(CPACK_DEBIAN_CLIENTS-VERSIONED_PACKAGE_CONTROL_EXTRA
  ${CMAKE_BINARY_DIR}/packaging/multiversion/clients/postinst
  ${CMAKE_BINARY_DIR}/packaging/multiversion/clients/prerm)
set(CPACK_DEBIAN_SERVER-VERSIONED_PACKAGE_CONTROL_EXTRA
  ${CMAKE_BINARY_DIR}/packaging/multiversion/server/postinst
  ${CMAKE_BINARY_DIR}/packaging/multiversion/server/prerm)

################################################################################
# Configuration for DEB
################################################################################

set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
set(CPACK_ARCHIVE_CLIENTS-TGZ_FILE_NAME "${deb-clients-filename}.${CMAKE_SYSTEM_PROCESSOR}")
set(CPACK_ARCHIVE_SERVER-TGZ_FILE_NAME "${deb-server-filename}.${CMAKE_SYSTEM_PROCESSOR}")

################################################################################
# Server configuration
################################################################################

string(RANDOM LENGTH 8 description1)
string(RANDOM LENGTH 8 description2)
set(CLUSTER_DESCRIPTION1 ${description1} CACHE STRING "Cluster description")
set(CLUSTER_DESCRIPTION2 ${description2} CACHE STRING "Cluster description")

if(NOT WIN32)
  fdb_install(FILES ${CMAKE_SOURCE_DIR}/packaging/foundationdb.conf
    DESTINATION etc
    COMPONENT server)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/make_public.py
    DESTINATION "usr/lib/foundationdb"
    COMPONENT server-deb)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/rpm/foundationdb.service
    DESTINATION "lib/systemd/system"
    COMPONENT server-el7)
  install(PROGRAMS ${CMAKE_SOURCE_DIR}/packaging/deb/foundationdb-init
    DESTINATION "etc/init.d"
    RENAME "foundationdb"
    COMPONENT server-deb)
  install(FILES ${CMAKE_SOURCE_DIR}/packaging/rpm/foundationdb.service
    DESTINATION "usr/lib/foundationdb-${FDB_VERSION}/lib/systemd/system"
    COMPONENT server-versioned)
  install(PROGRAMS ${CMAKE_SOURCE_DIR}/packaging/deb/foundationdb-init
    DESTINATION "usr/lib/foundationdb-${FDB_VERSION}/etc/init.d"
    RENAME "foundationdb"
    COMPONENT server-versioned)
endif()
