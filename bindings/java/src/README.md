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