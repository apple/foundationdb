function(fdb_setup_googlebenchmark)
  if(TARGET fdb_google_benchmark)
    return()
  endif()

  if(NOT benchmark_ROOT)
    if(EXISTS /opt/googlebenchmark-f91b6b AND CLANG AND USE_LIBCXX)
      set(benchmark_ROOT /opt/googlebenchmark-f91b6b)
    elseif(EXISTS /opt/googlebenchmark-f91b6b-g++ AND NOT USE_LIBCXX)
      set(benchmark_ROOT /opt/googlebenchmark-f91b6b-g++)
    endif()
  endif()

  find_package(benchmark)

  add_library(fdb_google_benchmark INTERFACE)

  if(benchmark_FOUND)
    target_link_libraries(fdb_google_benchmark INTERFACE benchmark::benchmark)
    target_include_directories(
      fdb_google_benchmark
      INTERFACE $<TARGET_PROPERTY:benchmark::benchmark,INTERFACE_INCLUDE_DIRECTORIES>)
    return()
  endif()

  if(NOT TARGET benchmark)
    set(googlebenchmark_root ${CMAKE_BINARY_DIR}/googlebenchmark-download)

    configure_file(
      ${CMAKE_SOURCE_DIR}/cmake/benchmark-download.cmake
      ${googlebenchmark_root}/CMakeLists.txt
      COPYONLY)

    execute_process(
      COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
      RESULT_VARIABLE results
      WORKING_DIRECTORY ${googlebenchmark_root})
    if(results)
      message(FATAL_ERROR "Configuration step for Benchmark has Failed. ${results}")
    endif()

    execute_process(
      COMMAND ${CMAKE_COMMAND} --build . --config Release
      RESULT_VARIABLE results
      WORKING_DIRECTORY ${googlebenchmark_root})
    if(results)
      message(FATAL_ERROR "Build step for Benchmark has Failed. ${results}")
    endif()

    set(BENCHMARK_ENABLE_TESTING OFF)
    add_subdirectory(
      ${googlebenchmark_root}/googlebenchmark-src
      ${googlebenchmark_root}/googlebenchmark-build
      EXCLUDE_FROM_ALL)
  endif()

  target_include_directories(
    fdb_google_benchmark
    INTERFACE ${CMAKE_BINARY_DIR}/googlebenchmark-download/googlebenchmark-src/include)
  target_link_libraries(fdb_google_benchmark INTERFACE benchmark)
endfunction()
