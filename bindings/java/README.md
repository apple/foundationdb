<img alt="FoundationDB logo" src="documentation/FDB_logo.png?raw=true" width="400">

FoundationDB is a distributed database designed to handle large volumes of structured data across clusters of commodity servers. It organizes data as an ordered key-value store and employs ACID transactions for all operations. It is especially well-suited for read/write workloads but also has excellent performance for write-intensive workloads. Users interact with the database using API language binding.

To learn more about FoundationDB, visit [foundationdb.org](https://www.foundationdb.org/)

## FoundationDB Java Bindings

In order to build the java bindings,
[JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html) >= 8
has to be installed. CMake will try to find a JDK installation, if it can find
one it will automatically build the java bindings.

If you have Java installed but cmake fails to find them, set the
`JAVA_HOME`environment variable.

### Fat Jar

By default, the generated jar file will depend on an installed libfdb_java
(provided with the generated RPM/DEB file on Linux). However, users usually find
a Jar-file that contains this library more convenient. This is also what you
will get if you download the jar file from Maven.

If you want to build a jar file that contains the library enable the cmake
variable `BUILD_FAT_JAR`. You can do this with the following command:

```
cmake -DBUILD_FAT_JAR <PATH_TO_FDB_SOURCE>
```

This will add the jni library of for the current architecture to the jar file.

#### Multi-Platform Jar-File

If you want to create a jar file that can run on more than one supported
architecture (the offical one supports MacOS, Linux, and Windows), you can do
that by executing the following steps:

1. Create a directory called `lib` somewhere on your file system.
1. Create a subdirectory for each *additional* platform you want to support
   (`windows` for windows, `osx` for MacOS, and `linux` for Linux).
1. Under each of those create a subdirectory with the name of the architecture
   (currently only `amd64` is supported - on MacOS this has to be called
   `x86_64` - `amd64` on all others).
1. Set the cmake variable `FAT_JAR_BINARIES` to this `lib` directory. For
   example, if you created this directory structure under `/foo/bar`, the
   corresponding cmake command would be:

```
cmake -DFAT_JAR_BINARIES=/foo/bar/lib <PATH_TO_FDB_SOURCE>
```

After executing building (with `make` or `Visual Studio`) you will find a
jar-file in the `fat-jar` directory in your build directory.
