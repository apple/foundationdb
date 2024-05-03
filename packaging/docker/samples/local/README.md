# Local Docker-based FoundationDB Cluster

This contains a sample `docker-compose.yaml` and some simple startup and teardown
scripts for running a simple single-instance FoundationDB using the Docker image
specified in this repository. This uses the `host` networking option to expose
the server process to its host machine.

This depends on having the FoundationDB client installed on your host machine
to work properly. This can be done using one of the client packages available
from our [GitHub Releases](https://github.com/apple/foundationdb/releases). The startup
scripts included here depend on `fdbcli` from one of those packages, and any
client that wishes to connect will need a copy of the FoundationDB native client
in addition to its binding of choice. Both the CLI and the native client
are installed in all of our client packages

Once those dependencies are installed, one can build the FoundationDB Docker
image:

```
docker build --build-arg FDB_VERSION=6.1.8 -t foundationdb:6.1.8 ../..
```

Then one can start the cluster by running:

```
./start.bash
```

This starts up a single instance FoundationDB cluster using the `docker-compose.yaml`
and configures it as a new database. This will write the cluster file information to
`docker.cluster`. One should then be able to access the cluster through the CLI
or one of the bindings by using this cluster file. For example:

```
fdbcli --exec status -C docker.cluster
```

To stop the cluster, one can run:

```
./stop.bash
```

Note that all data are lost between reboots of the processes as they have not
been configured to use a persistent volume (but write to Docker's temporary file system).
