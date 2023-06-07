add_library(crc32 STATIC crc32.S crc32_wrapper.c crc32c.cpp)
if (CLANG)
	# This is necessary for clang since the compiler reports that crc32_align is
	# defined but not used. With -Werror, crc32 will not compile.
	# TODO: Remove this when the upstream issue is repaired.
	target_compile_options(crc32 PRIVATE -Wno-unused-function)
endif ()
target_include_directories(crc32 PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
