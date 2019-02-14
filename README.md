<img alt="FoundationDB logo" src="documentation/FDB_logo.png?raw=true" width="400">

FoundationDB is a distributed database designed to handle large volumes of structured data across clusters of commodity servers. It organizes data as an ordered key-value store and employs ACID transactions for all operations. It is especially well-suited for read/write workloads but also has excellent performance for write-intensive workloads. Users interact with the database using API language binding.

To learn more about FoundationDB, visit [foundationdb.org](https://www.foundationdb.org/)

## Documentation

Documentation can be found online at <https://apple.github.io/foundationdb/>. The documentation covers details of API usage, background information on design philosophy, and extensive usage examples. Docs are built from the [source in this repo](documentation/sphinx/source).

## Forums

[The FoundationDB Forums](https://forums.foundationdb.org/) are the home for most of the discussion and communication about the FoundationDB project. We welcome your participation!  We want FoundationDB to be a great project to be a part of and, as part of that, have established a [Code of Conduct](CODE_OF_CONDUCT.md) to establish what constitutes permissible modes of interaction.

## Contributing

Contributing to FoundationDB can be in contributions to the code base, sharing your experience and insights in the community on the Forums, or contributing to projects that make use of FoundationDB. Please see the [contributing guide](CONTRIBUTING.md) for more specifics.

## Getting Started

### Binary downloads

Developers interested in using the FoundationDB store for an application can get started easily by downloading and installing a binary package. Please see the [downloads page](https://www.foundationdb.org/download/) for a list of available packages.


### Compiling from source

Developers on a OS for which there is no binary package, or who would like to start hacking on the code can get started by compiling from source.

Currently there are two build systems: a collection of Makefiles and a
CMake-based. Both of them should work for most users and CMake will eventually
become the only build system available.

## Makefile

#### MacOS

1. Check out this repo on your Mac.
1. Install the Xcode command-line tools.
1. Download version 1.52 of [Boost](https://sourceforge.net/projects/boost/files/boost/1.52.0/).
1. Set the `BOOSTDIR` environment variable to the location containing this boost installation.
1. Install [Mono](http://www.mono-project.com/download/stable/).
1. Install a [JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html). FoundationDB currently builds with Java 8.
1. Navigate to the directory where you checked out the foundationdb repo.
1. Run `make`.

#### Linux

1. Install [Docker](https://www.docker.com/).
1. Check out the foundationdb repo.
1. Build Linux docker image using the file `Dockerfile` located in the `build` source directory.

    ```shell
    cd /dir/path/foundationdb
    docker build ./build -t <image-tag-name>
    ```

1. Run the docker image interactively [Docker Run](https://docs.docker.com/engine/reference/run/#general-form) with the directory containing the foundationdb repo mounted [Docker Mounts](https://docs.docker.com/storage/volumes/).

    ```shell
    docker run -it -v '/local/dir/path/foundationdb:/docker/dir/path/foundationdb' <image-tag-name> /bin/bash
    ```

1. Navigate to the container's mounted directory which contains the foundationdb repo.

    ```shell
    cd /docker/dir/path/foundationdb
    ```
1. Run `make`.

This will build the fdbserver binary and the python bindings. If you want to build our other bindings, you will need to install a runtime for the language whose binding you want to build. Each binding has an `.mk` file which provides specific targets for that binding.

## CMake

FoundationDB is currently in the process of migrating the build system to cmake.
The CMake build system is currently used by several developers. However, most of
the testing and packaging infrastructure still uses the old VisualStudio+Make
based build system.

To build with CMake, generally the following is required (works on Linux and
Mac OS - for Windows see below):
1. Check out this repository.
2. Install cmake Version 3.12 or higher [CMake](https://cmake.org/)
1. Download version 1.67 of [Boost](https://sourceforge.net/projects/boost/files/boost/1.52.0/). 
1. Unpack boost (you don't need to compile it)
1. Install [Mono](http://www.mono-project.com/download/stable/).
1. Install a [JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html). FoundationDB currently builds with Java 8.
1. Create a build directory (you can have the build directory anywhere you
   like): `mkdir build`
1. `cd build`
1. `cmake -DBOOST_ROOT=<PATH_TO_BOOST> <PATH_TO_FOUNDATIONDB_DIRECTORY>`
1. `make`

CMake will try to find its dependencies. However, for LibreSSL this can be often
problematic (especially if OpenSSL is installed as well). For that we recommend
passing the argument `-DLibreSSL_ROOT` to cmake. So, for example, if you
LibreSSL is installed under /usr/local/libressl-2.8.3, you should call cmake like
this:

```
cmake -DLibreSSL_ROOT=/usr/local/libressl-2.8.3/ ../foundationdb
```

FoundationDB will build just fine without LibreSSL, however, the resulting
binaries won't support TLS connections.

### Generating compile_commands.json

CMake can build a compilation database for you. However, the default generatd
one is not too useful as it operates on the generated files. When running make,
the build system will create another `compile_commands.json` file in the source
directory. This can than be used for tools like
[CCLS](https://github.com/MaskRay/ccls),
[CQuery](https://github.com/cquery-project/cquery), etc. This way you can get
code-completion and code navigation in flow. It is not yet perfect (it will show
a few errors) but we are constantly working on improving the developement experience.

### Linux

There are no special requirements for Linux. However, we are currently working
on a Docker-based build as well.

If you want to create a package you have to tell cmake what platform it is for.
And then you can build by simply calling `cpack`. So for debian, call:

```
cmake -DINSTALL_LAYOUT=DEB  <FDB_SOURCE_DIR>
make
cpack
```

For RPM simply replace `DEB` with `RPM`.

### MacOS

The build under MacOS will work the same way as on Linux. To get LibreSSL and boost you
can use [Hombrew](https://brew.sh/). LibreSSL will not be installed in
`/usr/local` instead it will stay in `/usr/local/Cellar`. So the cmake command
will look somethink like this:

```
cmake -DLibreSSL_ROOT=/usr/local/Cellar/libressl/2.8.3 <PATH_TO_FOUNDATIONDB_SOURCE>
```

To generate a installable package, you have to call CMake with the corresponding
arguments and then use cpack to generate the package:

```
cmake -DINSTALL_LAYOUT=OSX  <FDB_SOURCE_DIR>
make
cpack
```

### Windows

Under Windows, the build instructions are very similar, with the main difference
that Visual Studio is used to compile.

1. Install Visual Studio 2017 (Community Edition is tested)
2. Install cmake Version 3.12 or higher [CMake](https://cmake.org/)
1. Download version 1.67 of [Boost](https://sourceforge.net/projects/boost/files/boost/1.52.0/). 
1. Unpack boost (you don't need to compile it)
1. Install [Mono](http://www.mono-project.com/download/stable/).
1. Install a [JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html). FoundationDB currently builds with Java 8.
1. Set JAVA_HOME to the unpacked location and JAVA_COMPILE to
   $JAVA_HOME/bin/javac
1. (Optional) Install [WIX](http://wixtoolset.org/). Without it Visual Studio
   won't build the Windows installer.
1. Create a build directory (you can have the build directory anywhere you
   like): `mkdir build`
1. `cd build`
1. `cmake -G "Visual Studio 15 2017 Win64" -DBOOST_ROOT=<PATH_TO_BOOST> <PATH_TO_FOUNDATIONDB_DIRECTORY>`
1. This should succeed. In which case you can build using msbuild:
   `msbuild /p:Configuration=Release fdb.sln`. You can also open the resulting
   solution in Visual Studio and compile from there. However, be aware that
   using Visual Studio for developement is currently not supported as Visual
   Studio will only know about the generated files.

If you want TLS support to be enabled under Windows you currently have to build
and install LibreSSL yourself as the newer LibreSSL versions are not provided
for download from the LibreSSL homepage. To build LibreSSL:

1. Download and unpack libressl (>= 2.8.2)
2. `cd libressl-2.8.2`
3. `mkdir build`
4. `cd build`
5. `cmake -G "Visual Studio 15 2017 Win64" ..`
6. Open the generated `LibreSSL.sln` in Visual Studio as administrator (this is
   necessary for the install)
7. Build the `INSTALL` project in `Release` mode

This will install LibreSSL under `C:\Program Files\LibreSSL`. After that `cmake`
will automatically find it and build with TLS support.

If you installed WIX before running `cmake` you should find the
`FDBInstaller.msi` in your build directory under `packaging/msi`. 
