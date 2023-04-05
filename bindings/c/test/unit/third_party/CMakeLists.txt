# Download doctest repo.
include(ExternalProject)
find_package(Git REQUIRED)

ExternalProject_Add(
    doctest_proj
    PREFIX ${CMAKE_BINARY_DIR}/doctest
    GIT_REPOSITORY https://github.com/onqtam/doctest.git
    GIT_TAG b7c21ec5ceeadb4951b00396fc1e4642dd347e5f # v2.4.9
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
