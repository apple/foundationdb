# Python sample using docker-compose

This contains a sample `docker-compose.yaml` to run a simple Python application running with FoundationDB as a storage backend.

All variables are located in `.env` file.

## Start the Python demo

```bash
docker-compose up -d
```

This will start:
* 1 coordinator,
* 2 fdbservers,
* 1 Flask application.

The Flask application can be accessed using curl:

```sh
# retrieve counter
curl http://0.0.0.0:5000/counter # 0

# increment counter
curl -X POST http://0.0.0.0:5000/counter/increment # 1
curl -X POST http://0.0.0.0:5000/counter/increment # 2

# retrieve counter
curl http://0.0.0.0:5000/counter # 2
```


## Access the FoundationDB cluster

If you want to access the cluster from your machine, here's a `cluster file` ready for you:

```bash
echo "docker:docker@127.0.0.1:4500" > docker.cluster
FDB_CLUSTER_FILE=./docker.cluster fdbcli
```

## Stop the Python demo

```
docker-compose down
```

## Use outside of docker-compose

The client application is also available as a standalone image: `foundationdb/foundationdb-sample-python-app:latest`.