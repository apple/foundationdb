# FoundationDB

FoundationDB is a distributed database designed to handle large volumes of structured data across clusters of commodity servers. It organizes data as an ordered key-value store and employs ACID transactions for all operations. It is especially well-suited for read/write workloads but also has excellent performance for write-intensive workloads. Users interact with the database using API language binding.

# Building Locally

## macOS

1. Check out this repo on your Mac.
1. Install the Xcode command-line tools.
1. Download version 1.52 of [Boost](https://sourceforge.net/projects/boost/files/boost/1.52.0/).
1. Set the BOOSTDIR environment variable to the location containing this boost installation.
1. Install [Mono](http://www.mono-project.com/download/stable/).
1. Install a [JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html). FoundationDB currently builds with Java 8.
1. Navigate to the directory where you checked out the foundationdb repo.
1. Run `make`.

## Linux

1. Install [Docker] (https://www.docker.com/) software
1. Build Linux docker image using the file `Dockerfile` located in the `build` source directory
1. Within the built Linux image, check out the foundationdb repo.
1. Run `make` within the source directory.

This will build the fdbserver binary and the python bindings. If you
want to build our other bindings, you will need to install a runtime for the
language whose binding you want to build. Each binding has an `.mk` file
which provides specific targets for that binding.
