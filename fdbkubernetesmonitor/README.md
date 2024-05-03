# FoundationDB Kubernetes monitor

This package provides a launcher program for running FoundationDB in Kubernetes.

## Testing

### Unit tests

You can run the unit tests by running `go test ./...` from this directory.

### Manual testing

To test this, run the following commands from the root of the FoundationDB repository:

```bash
mkdir -p website
docker build -t foundationdb/fdb-kubernetes-monitor:7.1.5 --target fdb-kubernetes-monitor --build-arg FDB_VERSION=7.1.5 --build-arg FDB_LIBRARY_VERSIONS="7.1.5 6.3.24 6.2.30" -f packaging/docker/Dockerfile .
docker build -t foundationdb/fdb-kubernetes-monitor:7.1.6 --target fdb-kubernetes-monitor --build-arg FDB_VERSION=7.1.6 --build-arg FDB_LIBRARY_VERSIONS="7.1.6 6.3.24 6.2.30" -f packaging/docker/Dockerfile .
kubectl apply -f packaging/docker/kubernetes/test_config.yaml
# Wait for the pods to become ready
kubectl rollout status sts/fdb-kubernetes-example
ips=$(kubectl get pod -l app=fdb-kubernetes-example -o json | jq -j '[[.items|.[]|select(.status.podIP!="")]|limit(3;.[])|.status.podIP+":4501"]|join(",")')
sed -e "s/fdb.cluster: \"\"/fdb.cluster: \"test:test@$ips\"/" -e "s/\"runProcesses\": false/\"runProcesses\": true/" packaging/docker/kubernetes/test_config.yaml | kubectl apply -f -
kubectl annotate pod -l app=fdb-kubernetes-example foundationdb.org/outdated-config-map-seen=$(date +%s) --overwrite
# Watch the logs for the fdb-kubernetes-example pods to confirm that they have launched the fdbserver processes.
kubectl exec -it sts/fdb-kubernetes-example -- fdbcli --exec "configure new double ssd"
```

This will set up a cluster in your Kubernetes environment using a StatefulSet, to provide a simple subset of what the Kubernetes operator does to set up the cluster.
Note: This assumes that you are running Docker Desktop on your local machine, with Kubernetes configured through Docker Desktop.

You can then make changes to the data in the ConfigMap and update the `fdbserver` processes:

```bash
sed -e "s/fdb.cluster: \"\"/fdb.cluster: \"test:test@$ips\"/" -e "s/\"runProcesses\": false/\"runProcesses\": true/" packaging/docker/kubernetes/test_config.yaml | kubectl apply -f -

# You can apply an annotation to speed up the propagation of config
kubectl annotate pod -l app=fdb-kubernetes-example foundationdb.org/outdated-config-map-seen=$(date +%s) --overwrite

# Watch the logs for the fdb-kubernetes-example pods to confirm that they have reloaded their configuration, and then do a bounce.
kubectl exec -it sts/fdb-kubernetes-example -- fdbcli --exec "kill; kill all; status"
```

Once you are done, you can tear down the example with the following command:

```bash
kubectl delete -f packaging/docker/kubernetes/test_config.yaml
kubectl delete pvc -l app=fdb-kubernetes-example
```

### FDB Kubernetes operator

The following steps assume that you already have a [local development](https://github.com/FoundationDB/fdb-kubernetes-operator#local-development) setup for the fdb-kubernetes-operator.

```bash
mkdir -p website
# Change this version if you want to create a cluster with a different version
docker build -t foundationdb/fdb-kubernetes-monitor:7.1.11 --target fdb-kubernetes-monitor --build-arg FDB_VERSION=7.1.11 --build-arg FDB_LIBRARY_VERSIONS="7.1.11 6.3.24 6.2.30" -f packaging/docker/Dockerfile .
```

Depending on the Kubernetes setup you use you might have to push the newly build image to a local registry.
Now you should change to the directory that contains the [fdb-kubernetes-operator](https://github.com/FoundationDB/fdb-kubernetes-operator) repository.
In the top directory run:

```bash
# Adjust the version for the cluster if it differs from the build image
vim ./config/tests/base/cluster.yaml
# Now you can create the cluster
kubectl apply -k ./config/tests/unified_image
```
