# Official FoundationDB Docker image

## What underlaying OSs are available?

* Ubuntu `18.04`, `16.04`
* CentOS `6.9`
* Debian `9.4` (not officially supported)


## Build

This is for development/testing purposes, official Docker builds are available on the Docker Hub.

```bash
git clone https://github.com/apple/foundationdb
docker build -t foundationdb:ubuntu-18.04 foundationdb/docker/ubuntu/18.04
```

Note: replace `ubuntu-18.04` and `ubuntu/18.04` with whatever version you are building.


## Usage

This will get you a Docker container running FoundationDB.

```bash
docker run -d \
  -v $(pwd)/conf:/etc/foundationdb \
  -v $(pwd)/logs:/var/log/foundationdb \
  -v $(pwd)/data:/var/lib/foundationdb/data \
  -p 127.0.0.1:4500:4500 \
  foundationdb:latest
```

* `-v $(pwd)/conf:/etc/foundationdb`, puts the configuration files on `./conf`.
* `-v $(pwd)/logs:/var/log/foundationdb`, puts the logs on `./logs`.
* `-v $(pwd)/data:/var/lib/foundationdb/data`, persistently stores database data on `./data`.
* `-p 127.0.0.1:4500:4500`, binds `localhost:4500/tcp` to the container's `4500/tcp`, for local access.

You should be able to connect to the FoundationDB container at `localhost:4500` with the `fdb.cluster` file in `./conf`.

Note: replace `latest` with any available distribution of this image.


## Usage with `docker-compose`

You can use this Docker container with `docker-compose`. Example:

```yaml
version: '3'

services:

  ... your app here ...

  db:
    image: foundationdb:latest
    volumes:
      - ./conf:/etc/foundationdb
      - ./logs:/var/log/foundationdb
      - ./data:/var/lib/foundationdb/data
```

Your app can now connect to FoundationDB at `db:4500` with the `fdb.cluster` file available in `./conf`.

Note: replace `latest` with any available distribution of this image.

Warning: `docker-compose` is not suitable for production environments.
