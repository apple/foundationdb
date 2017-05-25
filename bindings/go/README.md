fdb-go
======

[Go language](http://golang.org) bindings for [FoundationDB](http://foundationdb.org/documentation/), a distributed key-value store with ACID transactions.

This package requires:

- Go 1.1+ with CGO enabled
- FoundationDB C API 2.0.x, 3.0.x, or 4.x.y (part of the [FoundationDB clients package](https://files.foundationdb.org/fdb-c/))

Use of this package requires the selection of a FoundationDB API version at runtime. This package currently supports FoundationDB API versions 200-500.

To install this package, in the top level of this repository run:

    make fdb_go

Documentation
-------------

* [API documentation](https://foundationdb.org/documentation/godoc/fdb.html)
* [Tutorial](https://foundationdb.org/documentation/class-scheduling-go.html)
