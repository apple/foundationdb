# Overview

This directory provides various Docker images for running FoundationDB.

This directory includes two sets of images.  The "release" images are based
on Ubuntu 18.04.  The EKS images use Amazon Linux, which allows us to profile
FoundationDB when it is running inside of Amazon EKS.

# Build Configuration

The build scripts are configured using the following environment variables:

`TAG` is the base docker tag for this build.  The sidecar tag will be this
string, with a "-1" appended to it.  If you do not specify a tag, then the
scripts attempt to provide a reasonable default.

`ECR` is the name of the Docker registry the images should be published to.
It defaults to a private registry, so it is likely you will need to override this.

`STRIPPED` if true, the Docker images will contain stripped binaries without
debugging symbols.  Debugging symbols add approximately 2GiB to the image size.

# Release Dockerfile arguments.

These arguments are set automatically by the build scripts, but are documented here
in case you need to invoke the release Dockerfiles directly.

### FDB_VERSION

The version of FoundationDB to install in the container. This is required.

### FDB_WEBSITE

The base URL for the FoundationDB website. The default is
`https://www.foundationdb.org`.

You can build the docker without talking to a webserver by using the URL
`file:///mnt/website` and mirroring the directory tree of the webserver
inside the `website` subdirectory.

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

### FDB_COORDINATOR_PORT

The port to use for connecting to the FDB coordinator process. This should be
set by other processes in a multi-process cluster to the same value as the
`FDB_PORT` environment variable of the coordinator process. It will default
to 4500, which is also the default for `FDB_PORT`.

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

If you are running FDB inside of a Kubernetes cluster, you should probably use
the sidecar image instead.  It makes it easier to automatically copy a compatible
`libfdb_c.so` and cluster file into application containers.

TODO: Document sidecar.py

# Example Usages

### Build an Ubuntu-based image with a custom tag and unstripped binaries
```
# compile FDB, then:
cd ~/build_output/packages/docker/
TAG=my-custom-tag ./build-release-docker.sh
```
### Build an Amazon Linux-based image with a default tag and stripped binaries
```
# compile FDB, then:
cd ~/build_output/packages/docker/
STRIPPED=true ./build-eks-docker.sh
```