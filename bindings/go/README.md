fdb-go
======

[Go language](http://golang.org) bindings for [FoundationDB](https://apple.github.io/foundationdb/index.html#documentation), a distributed key-value store with ACID transactions.

This package requires:

- Go 1.22+ with CGO enabled
- FoundationDB client package (can be installed from the [release](https://github.com/apple/foundationdb/releases)). It's recommended to install the matching client package for the FDB version you want to use.

Use of this package requires the selection of a FoundationDB API version at runtime.
This package currently supports FoundationDB API versions 200-740.

Documentation
-------------

* [API documentation](https://godoc.org/github.com/apple/foundationdb/bindings/go/src/fdb)
* [Tutorial](https://apple.github.io/foundationdb/class-scheduling-go.html)

Modules
-------

In your go project with modules run the following command to get/update the required FDB version:

```bash
# Example for FDB 7.3.63 bindings
go get github.com/apple/foundationdb/bindings/go@7.3.63
```

This will add or update the bindings entry in your `go.mod` file like this:

```text
github.com/apple/foundationdb/bindings/go v0.0.0-20250221231555-5140696da2df
```

Note:  `@version` should match the major and minor of the FDB version you want to use, e.g. all 7.3 bindings will work for an FDB cluster in the 7.3 version.

Developing
----------

You can build this package, in the top level of this repository run:

```text
make fdb_go
```

This will create binary packages for the appropriate platform within the "build" subdirectory of this folder.
