if(UNIX AND NOT APPLE)
  add_library(folly_memcpy STATIC folly_memcpy.S)
  target_include_directories(folly_memcpy PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
endif()
