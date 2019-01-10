include(UseJava)
find_package(JNI 1.8 REQUIRED)
find_package(Java 1.8 COMPONENTS Development REQUIRED)

set(JAVA_BINDING_SRCS
  src/main/com/apple/foundationdb/async/AsyncIterable.java
  src/main/com/apple/foundationdb/async/AsyncIterator.java
  src/main/com/apple/foundationdb/async/AsyncUtil.java
  src/main/com/apple/foundationdb/async/Cancellable.java
  src/main/com/apple/foundationdb/async/CloneableException.java
  src/main/com/apple/foundationdb/async/CloseableAsyncIterator.java
  src/main/com/apple/foundationdb/async/package-info.java
  src/main/com/apple/foundationdb/Cluster.java
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
  src/main/com/apple/foundationdb/FDB.java
  src/main/com/apple/foundationdb/FDBDatabase.java
  src/main/com/apple/foundationdb/FDBTransaction.java
  src/main/com/apple/foundationdb/FutureCluster.java
  src/main/com/apple/foundationdb/FutureDatabase.java
  src/main/com/apple/foundationdb/FutureKey.java
  src/main/com/apple/foundationdb/FutureResult.java
  src/main/com/apple/foundationdb/FutureResults.java
  src/main/com/apple/foundationdb/FutureStrings.java
  src/main/com/apple/foundationdb/FutureVersion.java
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
  src/main/com/apple/foundationdb/tuple/ByteArrayUtil.java
  src/main/com/apple/foundationdb/tuple/IterableComparator.java
  src/main/com/apple/foundationdb/tuple/package-info.java
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
  src/test/com/apple/foundationdb/test/TupleTest.java
  src/test/com/apple/foundationdb/test/VersionstampSmokeTest.java
  src/test/com/apple/foundationdb/test/WatchTest.java
  src/test/com/apple/foundationdb/test/WhileTrueTest.java)

set(GENERATED_JAVA_DIR ${CMAKE_CURRENT_BINARY_DIR}/src/main/com/foundationdb)
file(MAKE_DIRECTORY ${GENERATED_JAVA_DIR})

set(GENERATED_JAVA_FILES
  ${GENERATED_JAVA_DIR}/ClusterOptions.java
  ${GENERATED_JAVA_DIR}/ConflictRangeType.java
  ${GENERATED_JAVA_DIR}/DatabaseOptions.java
  ${GENERATED_JAVA_DIR}/MutationType.java
  ${GENERATED_JAVA_DIR}/NetworkOptions.java
  ${GENERATED_JAVA_DIR}/StreamingMode.java
  ${GENERATED_JAVA_DIR}/TransactionOptions.java
  ${GENERATED_JAVA_DIR}/FDBException.java)

add_custom_command(OUTPUT ${GENERATED_JAVA_FILES}
  COMMAND ${MONO_EXECUTABLE} ${VEXILLOGRAPHER_EXE} ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options java ${GENERATED_JAVA_DIR}
  DEPENDS ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options vexillographer
  COMMENT "Generate Java options")
add_custom_target(fdb_java_options DEPENDS ${GENERATED_JAVA_DIR}/StreamingMode.java)

set(SYSTEM_NAME "linux")
if (APPLE)
  set(SYSTEM_NAME "osx")
endif()

add_library(fdb_java SHARED fdbJNI.cpp)
message(DEBUG ${JNI_INCLUDE_DIRS})
message(DEBUG ${JNI_LIBRARIES})
target_include_directories(fdb_java PRIVATE ${JNI_INCLUDE_DIRS})
# libfdb_java.so is loaded by fdb-java.jar and doesn't need to depened on jvm shared libraries.
target_link_libraries(fdb_java PRIVATE fdb_c)
set_target_properties(fdb_java PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib/${SYSTEM_NAME}/amd64/)

set(CMAKE_JAVA_COMPILE_FLAGS "-source" "1.8" "-target" "1.8")
set(CMAKE_JNI_TARGET TRUE)
set(JAR_VERSION "${FDB_MAJOR}.${FDB_MINOR}.${FDB_REVISION}")
add_jar(fdb-java ${JAVA_BINDING_SRCS} ${GENERATED_JAVA_FILES}
  OUTPUT_DIR ${PROJECT_BINARY_DIR}/lib)
add_dependencies(fdb-java fdb_java_options fdb_java)
add_jar(foundationdb-tests SOURCES ${JAVA_TESTS_SRCS} INCLUDE_JARS fdb-java)
add_dependencies(foundationdb-tests fdb_java_options)

install_jar(fdb-java DESTINATION ${FDB_SHARE_DIR}/java COMPONENT clients)
install(TARGETS fdb_java DESTINATION ${FDB_LIB_DIR} COMPONENT clients)
