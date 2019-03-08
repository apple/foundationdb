set(error_msg
  ${CMAKE_SOURCE_DIR}/versions.h exists. This usually means that
  you did run `make` "(the old build system)" in this directory before.
  This can result in unexpected behavior. run `make clean` in the
  source directory to continue)
if(EXISTS "${FILE}")
  list(JOIN error_msg " " err)
  message(FATAL_ERROR "${err}")
endif()
