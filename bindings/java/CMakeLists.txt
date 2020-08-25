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
  src/main/com/apple/foundationdb/DirectBufferPool.java
  src/main/com/apple/foundationdb/FDB.java
  src/main/com/apple/foundationdb/FDBDatabase.java
  src/main/com/apple/foundationdb/FDBTransaction.java
  src/main/com/apple/foundationdb/FutureInt64.java
  src/main/com/apple/foundationdb/FutureKey.java
  src/main/com/apple/foundationdb/FutureResult.java
  src/main/com/apple/foundationdb/FutureResults.java
  src/main/com/apple/foundationdb/FutureStrings.java
  src/main/com/apple/foundationdb/FutureVoid.java
  src/main/com/apple/foundationdb/JNIUtil.java
  src/main/com/apple/foundationdb/KeySelector.java
  src/main/com/apple/foundationdb/KeyValue.java
  src/main/com/apple/foundationdb/LocalityUtil.java
  src/main/com/apple/foundationdb/NativeFuture.java
  src/main/com/apple/foundationdb/NativeObjectWrapper.java
  src/main/com/apple/foundationdb/OptionConsumer.java
  src/main/com/apple/foundationdb/OptionsSet.java
  src/main/com/apple/foundationdb/package-info.java
  src/main/com/apple/foundationdb/Range.java
  src/main/com/apple/foundationdb/RangeQuery.java
  src/main/com/apple/foundationdb/RangeResult.java
  src/main/com/apple/foundationdb/RangeResultInfo.java
  src/main/com/apple/foundationdb/RangeResultSummary.java
  src/main/com/apple/foundationdb/ReadTransaction.java
  src/main/com/apple/foundationdb/ReadTransactionContext.java
  src/main/com/apple/foundationdb/subspace/package-info.java
  src/main/com/apple/foundationdb/subspace/Subspace.java
  src/main/com/apple/foundationdb/Transaction.java
  src/main/com/apple/foundationdb/TransactionContext.java
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
  src/test/com/apple/foundationdb/test/DirectoryTest.java
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
endif()
target_include_directories(fdb_java PRIVATE ${JNI_INCLUDE_DIRS})
# libfdb_java.so is loaded by fdb-java.jar and doesn't need to depened on jvm shared libraries.
target_link_libraries(fdb_java PRIVATE fdb_c)
set_target_properties(fdb_java PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib/${SYSTEM_NAME}/amd64/)
if(APPLE)
  set_target_properties(fdb_java PROPERTIES SUFFIX ".jnilib")
endif()
set_target_properties(java_workloads PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/share/foundationdb")
target_link_libraries(java_workloads PUBLIC fdb_c ${JNI_LIBRARIES})
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
  OUTPUT_DIR ${PROJECT_BINARY_DIR}/lib VERSION ${CMAKE_PROJECT_VERSION} MANIFEST ${MANIFEST_FILE})
add_dependencies(fdb-java fdb_java_options fdb_java)

# TODO[mpilman]: The java RPM will require some more effort (mostly on debian). However,
# most people will use the fat-jar, so it is not clear how high this priority is.

#install_jar(fdb-java DESTINATION ${FDB_SHARE_DIR}/java COMPONENT java)
#install(TARGETS fdb_java DESTINATION ${FDB_LIB_DIR} COMPONENT java)

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
    set(lib_destination "osx/x86_64")
  else()
    set(lib_destination "linux/amd64")
  endif()
	set(lib_destination "${unpack_dir}/lib/${lib_destination}")
	set(jni_package "${CMAKE_BINARY_DIR}/packages/lib")
  file(MAKE_DIRECTORY ${lib_destination})
	file(MAKE_DIRECTORY ${jni_package})
  add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/lib_copied
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:fdb_java> ${lib_destination} &&
    ${CMAKE_COMMAND} -E copy $<TARGET_FILE:fdb_java> ${jni_package} &&
    ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/lib_copied
    COMMENT "Copy jni library for fat jar")
  add_custom_target(copy_lib DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/lib_copied)
  add_dependencies(copy_lib unpack_jar)
  if(NOT FDB_RELEASE)
    set(prerelease_string "-PRERELEASE")
  else()
    set(prerelease_string "")
  endif()
  set(target_jar ${jar_destination}/fdb-java-${CMAKE_PROJECT_VERSION}${prerelease_string}.jar)
  add_custom_command(OUTPUT ${target_jar}
    COMMAND ${Java_JAR_EXECUTABLE} cfm ${target_jar} ${unpack_dir}/META-INF/MANIFEST.MF .
    WORKING_DIRECTORY ${unpack_dir}
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/lib_copied
    COMMENT "Build ${target_jar}")
  add_jar(foundationdb-tests SOURCES ${JAVA_TESTS_SRCS} INCLUDE_JARS fdb-java)
  add_dependencies(foundationdb-tests fdb_java_options)
  set(tests_jar ${jar_destination}/fdb-java-${CMAKE_PROJECT_VERSION}${prerelease_string}-tests.jar)
  add_custom_command(OUTPUT ${tests_jar}
    COMMAND ${CMAKE_COMMAND} -E copy foundationdb-tests.jar "${tests_jar}"
    WORKING_DIRECTORY .
    DEPENDS foundationdb-tests
    COMMENT "Build ${tests_jar}")
  add_custom_target(fdb-java-tests ALL DEPENDS ${tests_jar})
  add_dependencies(fdb-java-tests foundationdb-tests)
  add_custom_target(fat-jar ALL DEPENDS ${target_jar})
  add_dependencies(fat-jar fdb-java)
  add_dependencies(fat-jar copy_lib)
  add_dependencies(packages fat-jar)
endif()
