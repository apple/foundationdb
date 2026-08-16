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

  if(NOT benchmark_FOUND)
    include(FetchContent)
    FetchContent_Declare(
      googlebenchmark
      GIT_REPOSITORY https://github.com/google/benchmark.git
      # If you change this, then be sure to also update the directory name (which contains this SHA)
      # in FDB's build environment and the prebuilt googlebenchmark package.
      GIT_TAG f91b6b42b1b9854772a90ae9501464a161707d1e # v1.6.0
      GIT_SHALLOW ON
      GIT_CONFIG advice.detachedHead=false)

    set(BENCHMARK_ENABLE_TESTING OFF)
    set(BENCHMARK_ENABLE_INSTALL OFF)
    FetchContent_MakeAvailable(googlebenchmark)

    # Google Benchmark turns on -Wthread-safety and -Werror for itself in release builds. Its
    # std::mutex wrapper in src/mutex.h doesn't survive that on recent clang/libc++, which annotate
    # std::mutex as a capability. It's a third party dependency, so don't fail the build over its
    # warnings.
    foreach(target IN ITEMS benchmark benchmark_main)
      target_compile_options(${target} PRIVATE -Wno-error)
      set_target_properties(${target} PROPERTIES EXCLUDE_FROM_ALL ON)
    endforeach()
  endif()

  add_library(fdb_google_benchmark INTERFACE)
  target_link_libraries(fdb_google_benchmark INTERFACE benchmark::benchmark)
endfunction()
