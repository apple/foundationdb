Adding JUnit tests
===

For java development, it's often useful to use JUnit for testing due to the excellent tooling support.

To add a new unit test, do the following:

1. Write your test
2. Add the test path to `tests.cmake`, using the relative path starting at `src`(for example, `src/junit/com/apple/foundationdb/tuple/ArrayUtilTests.java` will add the `ArrayUtilTests` test file).
3. re-run the build (both `cmake` and `make/xcode/ninja`)

To add a new integration test:

1. Write the test, using JUnit.
2. Add the test path to `tests.cmake`, using the relative path starting at `src` (i.e. `src/integration/com/apple/foundationdb/DirectoryTest.java`).
3. re-run the build (both `cmake` and `make/xcode/ninja`)

To run all unit and integration tests, execute `ctest .` from `${BUILD_DIR}/bindings/java`. 

To skip integration tests, execute `ctest -E integration` from `${BUILD_DIR}/bindings/java`.

To run _only_ integration tests, run `ctest -R integration` from `${BUILD_DIR}/bindings/java`.

There are lots of other useful `ctest` commands, which we don't need to get into here. For more information,
see the [https://cmake.org/cmake/help/v3.19/manual/ctest.1.html](ctest documentation).

### Multi-Client tests
Multi-Client tests are integration tests that can only be executed when multiple clusters are running. To write a multi-client
test, do the following:

1. Tag all tests that require multiple clients with `@Tag("MultiClient")`
2. Ensure that your tests have the `MultiClientHelper` extension present, and Registered as an extension
3. Ensure that your test class is in the the JAVA_INTEGRATION_TESTS list in `test.cmake`

( see `BasicMultiClientIntegrationTest` for a good reference example)

It is important to note that it requires significant time to start and stop 3 separate clusters; if the underying test takes a long time to run,
ctest will time out and kill the test. When that happens, there is no guarantee that the FDB clusters will be properly stopped! It is thus
in your best interest to ensure that all tests run in a relatively small amount of time, or have a longer timeout attached.

