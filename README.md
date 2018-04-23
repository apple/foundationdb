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

#### macOS

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
1. Run the docker image interactively [Docker Run](https://docs.docker.com/engine/reference/run/#general-form) with the directory containing the foundationdb repo mounted [Docker Mounts](https://docs.docker.com/storage/volumes/).
`docker run -it -v '/local/dir/path/foundationdb:/docker/dir/path/foundationdb' /bin/bash`
1. Navigate to the mounted directory containing the foundationdb repo.
`cd /docker/dir/path/foundationdb`
1. Run `make`.

This will build the fdbserver binary and the python bindings. If you want to build our other bindings, you will need to install a runtime for the language whose binding you want to build. Each binding has an `.mk` file which provides specific targets for that binding.
