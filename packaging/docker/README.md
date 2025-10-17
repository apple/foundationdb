# packaging/docker

This directory contains the pieces for building FoundationDB docker images.

`build-images.sh` will optionally take a single parameter that will be used as an
image tag postfix.

For more details it is best to read the `build-images.sh` shell script itself to
learn more about how the images are built.

For details about what is in the images, peruse `Dockerfile`

the `samples` directory is out of date, and anything therein should be used with
the expectation that it is, at least, partially (if not entirely) incorrect.

## Building images locally without the script

If you only want to build a custom container image based on an alrady released FDB version you run the following command from the root:

```bash
export REGISTRY=docker.io
export FDB_VERSION=7.3.63
docker build --build-arg FDB_VERSION=${FDB_VERSION} -t ${REGISTRY}/foundationdb/fdb-kubernetes-monitor:${FDB_VERSION} --target fdb-kubernetes-monitor -f ./packaging/docker/Dockerfile .
```

Or if you want to build the `foundationdb` image and not the `fdb-kubernetes-monitor`:

```bash
export REGISTRY=docker.io
export FDB_VERSION=7.3.63
docker build --build-arg FDB_VERSION=${FDB_VERSION} -t ${REGISTRY}/foundationdb/foundationdb:${FDB_VERSION} --target foundationdb -f ./packaging/docker/Dockerfile .
```
