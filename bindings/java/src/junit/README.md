Adding JUnit tests
===

For java development, it's often useful to use JUnit for testing due to the excellent tooling support.

To add a new JUnit test, do the following:

1. Write your test
2. Add the test path to `tests.cmake`, using the relative path starting at `com`(for example, `com/apple/foundationdb/tuple/ArrayUtilTests.java` will add the `ArrayUtilTests` test file).
3. re-run cmake in your build directory. This will ensure your test gets picked up.
4. from your build directory, navigate to `bindings/java`
5. run `ctest .`

This will run JUnit tests through the `ctest` framework that cmake supports easily.

# Appendix: Useful ctest commands for the Java developer

1. To display output for ctest:
    `ctest . --output-on-failure`
2. To run just a single test:
    `ctest -R {fully qualified class name}`
3. To fail fast (on the first test):
    `ctest . --stop-on-failure`
4. To re-run only failed tests:
    `ctest . --rerun-failed`
5. To make ctest run using multiple threads
    `ctest . -j{Num threads}`