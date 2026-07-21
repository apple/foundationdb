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

If you only want to build a custom container image based on an already released FDB version you run the following command from the root:

```bash
FDB_VERSION=7.3.79
docker build --build-arg FDB_VERSION=${FDB_VERSION} -t foundationdb:${FDB_VERSION} --target foundationdb ./packaging/docker
```

If you want to build the `fdb-kubernetes-monitor` image (which includes fdb binaries too),
you need to use the build-output directory, even if not using binaries from the build.
(Just the cmake configure step is enough to set this up.)

```bash
FDB_VERSION=7.3.79
docker build --build-arg FDB_VERSION=${FDB_VERSION} -t fdb-kubernetes-monitor:${FDB_VERSION} --target fdb-kubernetes-monitor .../build-output/packages/docker
```
