# Overview

This directory provides a Docker image for running FoundationDB.

The image in this directory is based on Ubuntu 18.04, but the commands and
scripts used to build it should be suitable for most other distros with small
tweaks to the installation of dependencies.

The image relies on the following dependencies:

*	bash
*	wget
*	dig
*	glibc

# Build Configuration

This image supports several build arguments for build-time configuration.

### FDB_VERSION

The version of FoundationDB to install in the container. This is required.

### FDB_WEBSITE

The base URL for the FoundationDB website. The default is
`https://www.foundationdb.org`.

### FDB_ADDITIONAL_VERSIONS

A list of additional client library versions to include in this image. These
libraries will be in a special multiversion library folder.

# Runtime Configuration

This image supports several environment variables for run-time configuration.

### FDB_PORT

The port that FoundationDB should bind to. The default is 4500. 

### FDB_NETWORKING_MODE

A networking mode that controls what address FoundationDB listens on. If this
is `container` (the default), then the server will listen on its public IP
within the docker network, and will only be accessible from other containers.

If this is `host`, then the server will listen on `127.0.0.1`, and will not be
accessible from other containers. You should use `host` networking mode if you
want to access your container from your host machine, and you should also
map the port to the same port on your host machine when you run the container.

### FDB_COORDINATOR

A name of another FDB instance to use as a coordinator process. This can be
helpful when setting up a larger cluster inside a docker network, for instance
when using Docker Compose. The name you provide must be resolvable through the
DNS on the container you are running.

# Copying Into Other Images

You can also use this image to provide files for images that are clients of a
FoundationDB cluster, by using the `from` argument of the `COPY` command. Some
files you may want to copy are:

*	`/usr/lib/libfdb_c.so`: The primary FoundationDB client library
*	`/usr/lib/fdb/multiversion/libfdb_*.so`: Additional versions of the client
	library, which you can use if you are setting up a multiversion client.
*	`/var/fdb/scripts/create_cluster_file.bash`: A script for setting up the
	cluster file based on an `FDB_COORDINATOR` environment variable.
*	`/usr/bin/fdbcli`: The FoundationDB CLI.