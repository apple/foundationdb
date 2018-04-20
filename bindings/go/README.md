fdb-go
======

[Go language](http://golang.org) bindings for [FoundationDB](https://apple.github.io/foundationdb/index.html#documentation), a distributed key-value store with ACID transactions.

This package requires:

- Go 1.1+ with CGO enabled
- FoundationDB C API 2.0.x, 3.0.x, or 4.x.y (part of the [FoundationDB clients package](https://apple.github.io/foundationdb/downloads.html#c))

Use of this package requires the selection of a FoundationDB API version at runtime. This package currently supports FoundationDB API versions 200-520.

To build this package, in the top level of this repository run:

    make fdb_go

This will create binary packages for the appropriate platform within the "build" subdirectory of this folder.

To install this package, you can run the "fdb-go-install.sh" script:

    ./fdb-go-install.sh install 

The "install" command of this script does not depend on the presence of the repo in general and will download the repository into
your local go path. Running "localinstall" instead of "install" will use the local copy here (with a symlink) instead
of downloading from the remote repository.

Documentation
-------------

* [API documentation](https://godoc.org/github.com/apple/foundationdb/bindings/go/src/fdb)
* [Tutorial](https://apple.github.io/foundationdb/class-scheduling.html)
