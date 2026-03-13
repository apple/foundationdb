add_library(linenoise STATIC linenoise.c)
target_include_directories(linenoise PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
set_target_properties(linenoise PROPERTIES C_CLANG_TIDY "")
