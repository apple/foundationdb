# Golang sample using docker-compose

This contains a sample `docker-compose.yaml` to run a simple golang application running with FoundationDB as a storage backend.

All variables are located in `.env` file.

## Start the golang demo

```bash
docker-compose up -d
```

This will start:

* 1 coordinator,
* 2 fdbservers,
* a golang application. 

You can now head to [http://localhost:8080/counter](http://localhost:8080/counter) and see the counter rising-up after each refresh.

## Access the FoundationDB cluster

If you want to access the cluster from your machine, here's a `cluster file` ready for you:

```bash
echo "docker:docker@127.0.0.1:4500" > docker.cluster
FDB_CLUSTER_FILE=./docker.cluster fdbcli
```

## Stop the golang demo

```
docker-compose down
```