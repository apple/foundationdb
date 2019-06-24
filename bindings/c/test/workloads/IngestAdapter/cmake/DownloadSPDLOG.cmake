include(ExternalProject)
set(spdlog_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/external/spdlog)
ExternalProject_add(spdlogProject
  GIT_REPOSITORY "https://github.com/gabime/spdlog.git"
  # TODO: add patch to tweak.h to turn on per thread tracing
  # Most recent v1.x branch as of 4/24/2019
  GIT_TAG "366935142780e3754b358542f0fd5ed83793b263"
  PREFIX ${spdlog_PREFIX}
  BUILD_COMMAND echo "Skipping build step."
INSTALL_COMMAND cmake -E echo "Skipping install step."
  )

set(SPDLOG_INCLUDE ${spdlog_PREFIX}/src/spdlogProject/include)

add_library(spdlog INTERFACE)
add_dependencies(spdlog spdlogProject)
target_include_directories(spdlog INTERFACE ${SPDLOG_INCLUDE})
