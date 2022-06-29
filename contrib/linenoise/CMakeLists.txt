add_library(linenoise STATIC linenoise.c)
target_include_directories(linenoise PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
