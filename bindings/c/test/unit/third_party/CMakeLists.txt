find_package(doctest 2.4.8 CONFIG)
if(doctest_FOUND)
    find_path(DOCTEST_INCLUDE_DIR NAMES doctest/doctest.h PATH_SUFFIXES include)
    add_library(doctest INTERFACE)
    target_include_directories(doctest INTERFACE "${DOCTEST_INCLUDE_DIR}/doctest")
elseif(EXISTS /opt/doctest_proj_2.4.8)
    set(DOCTEST_INCLUDE_DIR "/opt/doctest_proj_2.4.8/doctest" CACHE INTERNAL "Path to include folder for doctest")
    add_library(doctest INTERFACE)
    add_dependencies(doctest doctest_proj)
    target_include_directories(doctest INTERFACE "${DOCTEST_INCLUDE_DIR}")
else()
    # Download doctest repo.
    include(ExternalProject)
    find_package(Git REQUIRED)

    ExternalProject_Add(
        doctest_proj
        PREFIX ${CMAKE_BINARY_DIR}/doctest
        GIT_REPOSITORY https://github.com/onqtam/doctest.git
        GIT_TAG 7b9885133108ae301ddd16e2651320f54cafeba7 # v2.4.8
        TIMEOUT 10
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        LOG_DOWNLOAD ON
    )

    ExternalProject_Get_Property(doctest_proj source_dir)
    set(DOCTEST_INCLUDE_DIR "${source_dir}/doctest" CACHE INTERNAL "Path to include folder for doctest")
    add_library(doctest INTERFACE)
    add_dependencies(doctest doctest_proj)
    target_include_directories(doctest INTERFACE "${DOCTEST_INCLUDE_DIR}")
endif()
