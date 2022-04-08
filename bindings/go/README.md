fdb-go
======

[Go language](http://golang.org) bindings for [FoundationDB](https://apple.github.io/foundationdb/index.html#documentation), a distributed key-value store with ACID transactions.

This package requires:

- Go 1.11+ with CGO enabled
- [Mono](http://www.mono-project.com/) (macOS or Linux) or [Visual Studio](https://www.visualstudio.com/) (Windows)  (build-time only)
- FoundationDB C API 2.0.x-6.1.x (part of the [FoundationDB client packages](https://apple.github.io/foundationdb/downloads.html#c))

Use of this package requires the selection of a FoundationDB API version at runtime. This package currently supports FoundationDB API versions 200-720.

To install this package, you can run the "fdb-go-install.sh" script (for versions 5.0.x and greater):

    ./fdb-go-install.sh install --fdbver <x.y.z>

The "install" command of this script does not depend on the presence of the repo in general and will download the repository into
your local go path. Running "localinstall" instead of "install" will use the local copy here (with a symlink) instead
of downloading from the remote repository.

You can also build this package, in the top level of this repository run:

    make fdb_go

This will create binary packages for the appropriate platform within the "build" subdirectory of this folder.


Documentation
-------------

* [API documentation](https://godoc.org/github.com/apple/foundationdb/bindings/go/src/fdb)
* [Tutorial](https://apple.github.io/foundationdb/class-scheduling-go.html)

Modules
-------

If you used the bindings with modules before the addition of the `go.mod` file in the foundation repo,
it may be necessary to update the import path in your `go.mod`.

By default, a module enabled `go get` will add something like this to your `go.mod`:
    
    github.com/apple/foundationdb vx.x.x-xxxxxxxxxxxxxx-xxxxxxxxxxxx

You will need to delete that line, then run `go get github.com/apple/foundationdb/bindings/go@version`.
You should now have a line like this in your `go.mod`:

    github.com/apple/foundationdb/bindings/go vx.x.x-xxxxxxxxxxxxxx-xxxxxxxxxxxx

Note:  `@version` is only necessary if you previously locked to a 
specific version or commit, in which case you'd replace `version` with a commit hash or tag.
