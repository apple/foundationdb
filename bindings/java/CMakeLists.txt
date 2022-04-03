set(RUN_JAVA_TESTS ON CACHE BOOL "Run Java unit tests")
set(RUN_JUNIT_TESTS OFF CACHE BOOL "Compile and run junit tests")
set(RUN_JAVA_INTEGRATION_TESTS OFF CACHE BOOL "Compile and run integration tests")

set(JAVA_BINDING_SRCS
  src/main/com/apple/foundationdb/async/AsyncIterable.java
  src/main/com/apple/foundationdb/async/AsyncIterator.java
  src/main/com/apple/foundationdb/async/AsyncUtil.java
  src/main/com/apple/foundationdb/async/Cancellable.java
  src/main/com/apple/foundationdb/async/CloneableException.java
  src/main/com/apple/foundationdb/async/CloseableAsyncIterator.java
  src/main/com/apple/foundationdb/async/package-info.java
  src/main/com/apple/foundationdb/Cluster.java
  src/main/com/apple/foundationdb/ClusterOptions.java
  src/main/com/apple/foundationdb/Database.java
  src/main/com/apple/foundationdb/directory/Directory.java
  src/main/com/apple/foundationdb/directory/DirectoryAlreadyExistsException.java
  src/main/com/apple/foundationdb/directory/DirectoryException.java
  src/main/com/apple/foundationdb/directory/DirectoryLayer.java
  src/main/com/apple/foundationdb/directory/DirectoryMoveException.java
  src/main/com/apple/foundationdb/directory/DirectoryPartition.java
  src/main/com/apple/foundationdb/directory/DirectorySubspace.java
  src/main/com/apple/foundationdb/directory/DirectoryUtil.java
  src/main/com/apple/foundationdb/directory/DirectoryVersionException.java
  src/main/com/apple/foundationdb/directory/MismatchedLayerException.java
  src/main/com/apple/foundationdb/directory/NoSuchDirectoryException.java
  src/main/com/apple/foundationdb/directory/package-info.java
  src/main/com/apple/foundationdb/directory/PathUtil.java
  src/main/com/apple/foundationdb/DirectBufferIterator.java
  src/main/com/apple/foundationdb/RangeResultDirectBufferIterator.java
  src/main/com/apple/foundationdb/MappedRangeResultDirectBufferIterator.java
  src/main/com/apple/foundationdb/DirectBufferPool.java
  src/main/com/apple/foundationdb/FDB.java
  src/main/com/apple/foundationdb/FDBDatabase.java
  src/main/com/apple/foundationdb/FDBTransaction.java
  src/main/com/apple/foundationdb/FutureInt64.java
  src/main/com/apple/foundationdb/FutureKey.java
  src/main/com/apple/foundationdb/FutureKeyArray.java
  src/main/com/apple/foundationdb/FutureResult.java
  src/main/com/apple/foundationdb/FutureResults.java
  src/main/com/apple/foundationdb/FutureMappedResults.java
  src/main/com/apple/foundationdb/FutureStrings.java
  src/main/com/apple/foundationdb/FutureVoid.java
  src/main/com/apple/foundationdb/JNIUtil.java
  src/main/com/apple/foundationdb/KeySelector.java
  src/main/com/apple/foundationdb/KeyValue.java
  src/main/com/apple/foundationdb/MappedKeyValue.java
  src/main/com/apple/foundationdb/LocalityUtil.java
  src/main/com/apple/foundationdb/NativeFuture.java
  src/main/com/apple/foundationdb/NativeObjectWrapper.java
  src/main/com/apple/foundationdb/OptionConsumer.java
  src/main/com/apple/foundationdb/OptionsSet.java
  src/main/com/apple/foundationdb/package-info.java
  src/main/com/apple/foundationdb/Range.java
  src/main/com/apple/foundationdb/RangeQuery.java
  src/main/com/apple/foundationdb/MappedRangeQuery.java
  src/main/com/apple/foundationdb/KeyArrayResult.java
  src/main/com/apple/foundationdb/RangeResult.java
  src/main/com/apple/foundationdb/MappedRangeResult.java
  src/main/com/apple/foundationdb/RangeResultInfo.java
  src/main/com/apple/foundationdb/MappedRangeResultInfo.java
  src/main/com/apple/foundationdb/RangeResultSummary.java
  src/main/com/apple/foundationdb/ReadTransaction.java
  src/main/com/apple/foundationdb/ReadTransactionContext.java
  src/main/com/apple/foundationdb/subspace/package-info.java
  src/main/com/apple/foundationdb/subspace/Subspace.java
  src/main/com/apple/foundationdb/Transaction.java
  src/main/com/apple/foundationdb/TransactionContext.java
  src/main/com/apple/foundationdb/EventKeeper.java
  src/main/com/apple/foundationdb/MapEventKeeper.java
  src/main/com/apple/foundationdb/testing/AbstractWorkload.java
  src/main/com/apple/foundationdb/testing/WorkloadContext.java
  src/main/com/apple/foundationdb/testing/Promise.java
  src/main/com/apple/foundationdb/testing/PerfMetric.java
  src/main/com/apple/foundationdb/tuple/ByteArrayUtil.java
  src/main/com/apple/foundationdb/tuple/FastByteComparisons.java
  src/main/com/apple/foundationdb/tuple/IterableComparator.java
  src/main/com/apple/foundationdb/tuple/package-info.java
  src/main/com/apple/foundationdb/tuple/StringUtil.java
  src/main/com/apple/foundationdb/tuple/Tuple.java
  src/main/com/apple/foundationdb/tuple/TupleUtil.java
  src/main/com/apple/foundationdb/tuple/Versionstamp.java)

set(JAVA_TESTS_SRCS
  src/test/com/apple/foundationdb/test/AbstractTester.java
  src/test/com/apple/foundationdb/test/AsyncDirectoryExtension.java
  src/test/com/apple/foundationdb/test/AsyncStackTester.java
  src/test/com/apple/foundationdb/test/BlockingBenchmark.java
  src/test/com/apple/foundationdb/test/ConcurrentGetSetGet.java
  src/test/com/apple/foundationdb/test/Context.java
  src/test/com/apple/foundationdb/test/ContinuousSample.java
  src/test/com/apple/foundationdb/test/DirectoryExtension.java
  src/test/com/apple/foundationdb/test/DirectoryOperation.java
  src/test/com/apple/foundationdb/test/DirectoryUtil.java
  src/test/com/apple/foundationdb/test/Example.java
  src/test/com/apple/foundationdb/test/Instruction.java
  src/test/com/apple/foundationdb/test/IterableTest.java
  src/test/com/apple/foundationdb/test/LocalityTests.java
  src/test/com/apple/foundationdb/test/ParallelRandomScan.java
  src/test/com/apple/foundationdb/test/PerformanceTester.java
  src/test/com/apple/foundationdb/test/RangeTest.java
  src/test/com/apple/foundationdb/test/RYWBenchmark.java
  src/test/com/apple/foundationdb/test/SerialInsertion.java
  src/test/com/apple/foundationdb/test/SerialIteration.java
  src/test/com/apple/foundationdb/test/SerialTest.java
  src/test/com/apple/foundationdb/test/Stack.java
  src/test/com/apple/foundationdb/test/StackEntry.java
  src/test/com/apple/foundationdb/test/StackOperation.java
  src/test/com/apple/foundationdb/test/StackTester.java
  src/test/com/apple/foundationdb/test/StackUtils.java
  src/test/com/apple/foundationdb/test/TesterArgs.java
  src/test/com/apple/foundationdb/test/TestResult.java
  src/test/com/apple/foundationdb/test/TuplePerformanceTest.java
  src/test/com/apple/foundationdb/test/TupleTest.java
  src/test/com/apple/foundationdb/test/VersionstampSmokeTest.java
  src/test/com/apple/foundationdb/test/WatchTest.java
  src/test/com/apple/foundationdb/test/WhileTrueTest.java)

include(src/tests.cmake)

set(GENERATED_JAVA_DIR ${CMAKE_CURRENT_BINARY_DIR}/src/main/com/apple/foundationdb)
file(MAKE_DIRECTORY ${GENERATED_JAVA_DIR})

set(GENERATED_JAVA_FILES
  ${GENERATED_JAVA_DIR}/ConflictRangeType.java
  ${GENERATED_JAVA_DIR}/DatabaseOptions.java
  ${GENERATED_JAVA_DIR}/MutationType.java
  ${GENERATED_JAVA_DIR}/NetworkOptions.java
  ${GENERATED_JAVA_DIR}/StreamingMode.java
  ${GENERATED_JAVA_DIR}/TransactionOptions.java
  ${GENERATED_JAVA_DIR}/FDBException.java)

vexillographer_compile(TARGET fdb_java_options LANG java OUT ${GENERATED_JAVA_DIR}
  OUTPUT ${GENERATED_JAVA_FILES})

set(SYSTEM_NAME "linux")
if (APPLE)
  set(SYSTEM_NAME "osx")
endif()

if(OPEN_FOR_IDE)
  add_library(fdb_java OBJECT fdbJNI.cpp)
  add_library(java_workloads OBJECT JavaWorkload.cpp)
else()
  add_library(fdb_java SHARED fdbJNI.cpp)
  add_library(java_workloads SHARED JavaWorkload.cpp)
  target_link_libraries(java_workloads PRIVATE fdb_java_native)
endif()

if (NOT WIN32 AND NOT APPLE AND NOT OPEN_FOR_IDE)
  target_link_options(java_workloads PRIVATE "LINKER:--version-script=${CMAKE_SOURCE_DIR}/bindings/c/external_workload.map,-z,nodelete")
endif()

target_include_directories(fdb_java PRIVATE ${JNI_INCLUDE_DIRS})
# libfdb_java.so is loaded by fdb-java.jar and doesn't need to depened on jvm shared libraries.
target_link_libraries(fdb_java PRIVATE fdb_c)
target_link_libraries(fdb_java PRIVATE fdb_java_native)
if(APPLE)
  set_target_properties(fdb_java PROPERTIES SUFFIX ".jnilib")
endif()
set_target_properties(java_workloads PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/share/foundationdb")
target_link_libraries(java_workloads PUBLIC fdb_c ${JNI_LIBRARIES})
target_link_libraries(java_workloads PRIVATE flow) # mostly for boost
target_include_directories(java_workloads PUBLIC ${JNI_INCLUDE_DIRS})

set(CMAKE_JAVA_COMPILE_FLAGS "-source" "1.8" "-target" "1.8" "-XDignore.symbol.file")
set(CMAKE_JNI_TARGET TRUE)

# build a manifest file
set(JAR_VERSION "${FDB_MAJOR}.${FDB_MINOR}.${FDB_PATCH}")
string(TIMESTAMP BND_LAST_MODIFIED_SEC "%s")
set(MANIFEST_TEXT "Manifest-Version: 1.0
Bnd-LastModified: ${BND_LAST_MODIFIED_SEC}000
Build-Jdk: ${Java_VERSION_STRING}
Built-By: CMake ${CMAKE_VERSION}
Bundle-Description: FoundationDB Java bindings and API
Bundle-ManifestVersion: 2
Bundle-Name: fdb-java
Bundle-SymbolicName: com.apple.foundationdb
Bundle-Version: ${JAR_VERSION}
Created-By: CMake ${CMAKE_VERSION}
Implementation-Title: fdb-java
Implementation-Version: ${CURRENT_GIT_VERSION}
Specification-Title: FoundationDB Java bindings and API
Specification-Version: ${JAR_VERSION}
")

# write the manifest file
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/resources/META-INF)
set(MANIFEST_FILE ${CMAKE_CURRENT_BINARY_DIR}/resources/META-INF/MANIFEST.MF)
file(WRITE ${MANIFEST_FILE} ${MANIFEST_TEXT})

add_jar(fdb-java ${JAVA_BINDING_SRCS} ${GENERATED_JAVA_FILES} ${CMAKE_SOURCE_DIR}/LICENSE
  OUTPUT_DIR ${PROJECT_BINARY_DIR}/lib VERSION ${FDB_VERSION} MANIFEST ${MANIFEST_FILE} GENERATE_NATIVE_HEADERS fdb_java_native)
add_dependencies(fdb-java fdb_java_options)

if(NOT OPEN_FOR_IDE)
  set(FAT_JAR_BINARIES "NOTFOUND" CACHE STRING
    "Path of a directory structure with libraries to include in fat jar (a lib directory)")

  set(jar_destination ${CMAKE_BINARY_DIR}/packages)
  set(unpack_dir ${CMAKE_CURRENT_BINARY_DIR}/fat_jar)
  file(MAKE_DIRECTORY ${jar_destination})
  file(MAKE_DIRECTORY ${unpack_dir})
  message(STATUS "Building fat jar to ${jar_destination}")
  get_property(jar_path TARGET fdb-java PROPERTY JAR_FILE)
  add_custom_command(OUTPUT ${unpack_dir}/META-INF/MANIFEST.MF
    COMMAND ${Java_JAR_EXECUTABLE} xf ${jar_path}
    WORKING_DIRECTORY ${unpack_dir}
    DEPENDS "${jar_path}"
    COMMENT "Unpack jar-file")
  add_custom_target(unpack_jar DEPENDS ${unpack_dir}/META-INF/MANIFEST.MF)
  add_dependencies(unpack_jar fdb-java)
  add_custom_command(OUTPUT ${unpack_dir}/LICENSE
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/LICENSE ${unpack_dir}
    COMMENT "copy license")
  add_custom_target(copy_license DEPENDS ${unpack_dir}/LICENSE)
  add_dependencies(unpack_jar copy_license)
  if(FAT_JAR_BINARIES)
    add_custom_command(OUTPUT ${unpack_dir}/lib
      COMMAND ${CMAKE_COMMAND} -E copy_directory ${FAT_JAR_BINARIES} ${unpack_dir}
      COMMENT "copy additional libraries"
      DEPENDS ${unpack_dir}/META-INF/MANIFEST.MF)
    add_custom_target(copy_libs DEPENDS ${unpack_dir}/lib)
    add_dependencies(unpack_jar copy_libs)
  endif()
  if(WIN32)
    set(lib_destination "windows/amd64")
  elseif(APPLE)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
      set(lib_destination "osx/aarch64")
    else()
      set(lib_destination "osx/x86_64")
    endif()
  else()
   if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
     set(lib_destination "linux/aarch64")
   elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc64le")
     set(lib_destination "linux/ppc64le")
   else()
     set(lib_destination "linux/amd64")
    endif()
  endif()
	set(lib_destination "${unpack_dir}/lib/${lib_destination}")
	set(jni_package "${CMAKE_BINARY_DIR}/packages/lib")
  file(MAKE_DIRECTORY ${lib_destination})
	file(MAKE_DIRECTORY ${jni_package})
  add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/lib_copied
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:fdb_java> ${lib_destination} &&
    ${CMAKE_COMMAND} -E copy $<TARGET_FILE:fdb_java> ${jni_package} &&
    ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/lib_copied
    DEPENDS fdb_java
    COMMENT "Copy jni library for fat jar")
  add_custom_target(copy_lib DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/lib_copied)
  add_dependencies(copy_lib unpack_jar)
  if(NOT FDB_RELEASE)
    set(not_fdb_release_string "-SNAPSHOT")
  else()
    set(not_fdb_release_string "")
  endif()
  set(target_jar ${jar_destination}/fdb-java-${FDB_VERSION}${not_fdb_release_string}.jar)
  add_custom_command(OUTPUT ${target_jar}
    COMMAND ${Java_JAR_EXECUTABLE} cfm ${target_jar} ${unpack_dir}/META-INF/MANIFEST.MF .
    WORKING_DIRECTORY ${unpack_dir}
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/lib_copied
    COMMENT "Build ${target_jar}")
  add_jar(foundationdb-tests SOURCES ${JAVA_TESTS_SRCS} INCLUDE_JARS fdb-java)
  add_dependencies(foundationdb-tests fdb_java_options)
  set(tests_jar ${jar_destination}/fdb-java-${FDB_VERSION}${not_fdb_release_string}-tests.jar)
  add_custom_command(OUTPUT ${tests_jar}
    COMMAND ${CMAKE_COMMAND} -E copy foundationdb-tests.jar "${tests_jar}"
    WORKING_DIRECTORY .
    DEPENDS foundationdb-tests
    COMMENT "Build ${tests_jar}")
  add_custom_target(fdb-java-tests ALL DEPENDS ${tests_jar})
  add_dependencies(fdb-java-tests foundationdb-tests)
  add_custom_target(fat-jar ALL DEPENDS ${target_jar})
  add_dependencies(fat-jar fdb-java)
  add_dependencies(fat-jar fdb_java)
  add_dependencies(fat-jar copy_lib)
  add_dependencies(packages fat-jar)

  set(TEST_CP ${tests_jar} ${target_jar})

  if(RUN_JUNIT_TESTS OR RUN_JAVA_INTEGRATION_TESTS)
    if (USE_SANITIZER)
      message(WARNING "Cannot run java tests with sanitizer builds")
      return()
    endif()
    # We use Junit libraries for both JUnit and integration testing structures, so download in either case
    # https://search.maven.org/remotecontent?filepath=org/junit/jupiter/junit-jupiter-engine/5.7.1/junit-jupiter-engine-5.7.1.jar
    file(DOWNLOAD "https://search.maven.org/remotecontent?filepath=org/junit/jupiter/junit-jupiter-engine/5.7.1/junit-jupiter-engine-5.7.1.jar"
      ${CMAKE_BINARY_DIR}/packages/junit-jupiter-engine-5.7.1.jar
      EXPECTED_HASH SHA256=56616c9350b3624f76cffef6b24ce7bb222915bfd5688f96d3cf4cef34f077cb)
      # https://search.maven.org/remotecontent?filepath=org/junit/jupiter/junit-jupiter-api/5.7.1/junit-jupiter-api-5.7.1.jar
    file(DOWNLOAD "https://search.maven.org/remotecontent?filepath=org/junit/jupiter/junit-jupiter-api/5.7.1/junit-jupiter-api-5.7.1.jar"
      ${CMAKE_BINARY_DIR}/packages/junit-jupiter-api-5.7.1.jar
      EXPECTED_HASH SHA256=ce7b985bc469e2625759a4ebc45533c70581a05a348278c1d6408e9b2e35e314)
    file(DOWNLOAD "https://search.maven.org/remotecontent?filepath=org/junit/jupiter/junit-jupiter-params/5.7.1/junit-jupiter-params-5.7.1.jar"
      ${CMAKE_BINARY_DIR}/packages/junit-jupiter-params-5.7.1.jar
      EXPECTED_HASH SHA256=8effdd7f8a4ba5558b568184dee08008b2443c86c673ef81de5861fbc7ef0613)
    file(DOWNLOAD "https://search.maven.org/remotecontent?filepath=org/junit/platform/junit-platform-commons/1.7.1/junit-platform-commons-1.7.1.jar"
      ${CMAKE_BINARY_DIR}/packages/junit-platform-commons-1.7.1.jar
      EXPECTED_HASH SHA256=7c546be86864718fbaceb79fa84ff1d3a516500fc428f1b21d061c2e0fbc5a4b)
    file(DOWNLOAD "https://search.maven.org/remotecontent?filepath=org/junit/platform/junit-platform-engine/1.7.1/junit-platform-engine-1.7.1.jar"
      ${CMAKE_BINARY_DIR}/packages/junit-platform-engine-1.7.1.jar
      EXPECTED_HASH SHA256=37df5a9cd6dbc1f754ba2b46f96b8874a83660e1796bf38c738f022dcf86c23f)
    file(DOWNLOAD "https://search.maven.org/remotecontent?filepath=org/junit/platform/junit-platform-launcher/1.7.1/junit-platform-launcher-1.7.1.jar"
      ${CMAKE_BINARY_DIR}/packages/junit-platform-launcher-1.7.1.jar
      EXPECTED_HASH SHA256=3122ac6fb284bc50e3afe46419fc977f94d580e9d3d1ea58805d200b510a99ee)
    file(DOWNLOAD "https://search.maven.org/remotecontent?filepath=org/junit/platform/junit-platform-console/1.7.1/junit-platform-console-1.7.1.jar"
      ${CMAKE_BINARY_DIR}/packages/junit-platform-console-1.7.1.jar
      EXPECTED_HASH SHA256=11ed48fcdfcea32f2fa98872db7ecba2d49d178f76493e7a149a2242363ad12e)
    file(DOWNLOAD "https://search.maven.org/remotecontent?filepath=org/apiguardian/apiguardian-api/1.1.1/apiguardian-api-1.1.1.jar"
      ${CMAKE_BINARY_DIR}/packages/apiguardian-api-1.1.1.jar
      EXPECTED_HASH SHA256=fc68f0d28633caccf3980fdf1e99628fba9c49424ee56dc685cd8b4d2a9fefde)
    file(DOWNLOAD "https://search.maven.org/remotecontent?filepath=org/opentest4j/opentest4j/1.2.0/opentest4j-1.2.0.jar"
      ${CMAKE_BINARY_DIR}/packages/opentest4j-1.2.0.jar)

    set(JUNIT_CLASSPATH "${CMAKE_BINARY_DIR}/packages/junit-jupiter-api-5.7.1.jar:${CMAKE_BINARY_DIR}/packages/junit-jupiter-engine-5.7.1.jar:${CMAKE_BINARY_DIR}/packages/junit-jupiter-params-5.7.1.jar:${CMAKE_BINARY_DIR}/packages/opentest4j-1.2.0.jar:${CMAKE_BINARY_DIR}/packages/apiguardian-api-1.1.1.jar")
    set(JUNIT_CLASSPATH "${JUNIT_CLASSPATH}:${CMAKE_BINARY_DIR}/packages/junit-platform-console-1.7.1.jar:${CMAKE_BINARY_DIR}/packages/junit-platform-commons-1.7.1.jar")
    set(JUNIT_CLASSPATH "${JUNIT_CLASSPATH}:${CMAKE_BINARY_DIR}/packages/junit-platform-engine-1.7.1.jar:${CMAKE_BINARY_DIR}/packages/junit-platform-launcher-1.7.1.jar")
  endif()

  if(RUN_JUNIT_TESTS)
    # Sets up the JUnit testing structure to run through ctest
    #
    # To add a new junit test, add the class to the JAVA_JUNIT_TESTS variable in `src/tests.cmake`. Note that if you run a Suite,
    # ctest will NOT display underlying details of the suite itself, so it's best to avoid junit suites in general. Also,
    # if you need a different runner other than JUnitCore, you'll have to modify this so be aware.
    #
    # To run tests (once built), run:
    #
    # ctest .
    #
    # from the ${BUILD_DIR}/bindings/java subdirectory.
    #
    # Note: if you are running from ${BUILD_DIR}, additional tests of the native logic will be run. To avoid these, use
    #
    # ctest . -R java-unit
    #
    # ctest has lots of flexible command options, so be sure to refer to its documentation if you want to do something specific(documentation
    # can be found at https://cmake.org/cmake/help/v3.19/manual/ctest.1.html)

    add_jar(fdb-junit SOURCES ${JAVA_JUNIT_TESTS} ${JUNIT_RESOURCES} INCLUDE_JARS fdb-java
            ${CMAKE_BINARY_DIR}/packages/junit-jupiter-api-5.7.1.jar
            ${CMAKE_BINARY_DIR}/packages/junit-jupiter-engine-5.7.1.jar
            ${CMAKE_BINARY_DIR}/packages/junit-jupiter-params-5.7.1.jar
            ${CMAKE_BINARY_DIR}/packages/opentest4j-1.2.0.jar
            ${CMAKE_BINARY_DIR}/packages/apiguardian-api-1.1.1.jar
    )
    get_property(junit_jar_path TARGET fdb-junit PROPERTY JAR_FILE)

    add_test(NAME java-unit
      COMMAND ${Java_JAVA_EXECUTABLE}
            -classpath "${target_jar}:${junit_jar_path}:${JUNIT_CLASSPATH}"
            -Djava.library.path=${CMAKE_BINARY_DIR}/lib
            org.junit.platform.console.ConsoleLauncher "--details=summary" "-class-path=${junit_jar_path}" "--scan-classpath" "--disable-banner"
    )

  endif()

  if(RUN_JAVA_INTEGRATION_TESTS)
    # Set up the integration tests. These tests generally require a running database server to function properly. Most tests
    # should be written such that they can be run in parallel with other integration tests (e.g. try to use a unique key range for each test
    # whenever possible), because it's a reasonable assumption that a single server will be shared among multiple tests, and might do so
    # concurrently.
    #
    # Integration tests are run through ctest the same way as unit tests, but their label is prefixed with the entry 'integration-'.
    # Note that most java integration tests will fail if they can't quickly connect to a running FDB instance(depending on how the test is written, anyway).
    # However, if you want to explicitly skip them, you can run
    #
    # `ctest -E integration`
    #
    # To run only integration tests, execute
    #
    # `ctest -R integration`
    #
    # (Note: both of these commands are assumed to be running from the ${BUILD_DIR}/bindings/java directory).
    #
    # To add an integration test, add the relative class file path to the JAVA_INTEGRATION_TESTS variable in `src/tests.cmake`
    #
    # All integration tests share the same fdb cluster, so you should design
    # your test with that in mind (e.g. don't depend on the database being
    # empty, consider generating a random prefix for the keys you write, use
    # the directory layer with a unique path, etc.)
    #
    add_jar(fdb-integration SOURCES ${JAVA_INTEGRATION_TESTS} ${JAVA_INTEGRATION_RESOURCES} INCLUDE_JARS fdb-java
            ${CMAKE_BINARY_DIR}/packages/junit-jupiter-api-5.7.1.jar
            ${CMAKE_BINARY_DIR}/packages/junit-jupiter-engine-5.7.1.jar
            ${CMAKE_BINARY_DIR}/packages/junit-jupiter-params-5.7.1.jar
            ${CMAKE_BINARY_DIR}/packages/opentest4j-1.2.0.jar
            ${CMAKE_BINARY_DIR}/packages/apiguardian-api-1.1.1.jar)
    get_property(integration_jar_path TARGET fdb-integration PROPERTY JAR_FILE)


    # add_fdbclient_test will set FDB_CLUSTER_FILE if it's not set already
    add_fdbclient_test(NAME java-integration
                    COMMAND ${Java_JAVA_EXECUTABLE}
                      -classpath "${target_jar}:${integration_jar_path}:${JUNIT_CLASSPATH}"
                      -Djava.library.path=${CMAKE_BINARY_DIR}/lib
                      org.junit.platform.console.ConsoleLauncher "--details=summary" "--class-path=${integration_jar_path}" "--scan-classpath" "--disable-banner" "-T MultiClient"
    )

    add_multi_fdbclient_test(NAME java-multi-integration
                    COMMAND ${Java_JAVA_EXECUTABLE}
                    -classpath "${target_jar}:${integration_jar_path}:${JUNIT_CLASSPATH}"
                    -Djava.library.path=${CMAKE_BINARY_DIR}/lib
                    org.junit.platform.console.ConsoleLauncher "--details=summary" "--class-path=${integration_jar_path}" "--scan-classpath" "--disable-banner" "-t MultiClient"
    )

  endif()
endif()
