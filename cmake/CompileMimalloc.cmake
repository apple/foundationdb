find_package(mimalloc 1.6.7 QUIET)

if(mimalloc_FOUND)
  add_library(Mimalloc INTERFACE)
  target_link_libraries(Mimalloc INTERFACE mimalloc-static)
else()
  include(ExternalProject)
  ExternalProject_add(mimallocProject
    URL "https://github.com/microsoft/mimalloc/archive/v1.6.7.tar.gz"
    URL_HASH SHA256=111b718b496f297f128d842880e72e90e33953cf00b45ba0ccd2167e7340ed17
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/mimalloc
                     -DCMAKE_BUILD_TYPE:STRING=Release -DMI_BUILD_TESTS:BOOL=OFF
                     -DMI_BUILD_SHARED:BOOL=OFF -DMI_BUILD_STATIC:BOOL=ON
    BUILD_BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/mimalloc/lib/mimalloc-1.6/libmimalloc.a"
    BUILD_ALWAYS ON)
  add_library(Mimalloc STATIC IMPORTED)
  add_dependencies(Mimalloc mimallocProject)
  set_target_properties(Mimalloc PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/mimalloc/lib/mimalloc-1.6/libmimalloc.a")
endif()
