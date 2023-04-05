add_library(sqlite STATIC
  btree.h
  hash.h
  sqlite3.h
  sqlite3ext.h
  sqliteInt.h
  sqliteLimit.h
  sqlite3.amalgamation.c)


target_include_directories(sqlite PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# Suppress warnings in sqlite since it's third party
if(NOT WIN32)
  target_compile_definitions(sqlite PRIVATE $<$<CONFIG:Debug>:NDEBUG>)
  target_compile_options(sqlite BEFORE PRIVATE -w) # disable warnings for third party
endif()

# SQLite won't build with -ffast-math
if(ICX)
  target_compile_options(sqlite PRIVATE -fno-fast-math)
endif()