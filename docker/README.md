# FoundationDB Docker image

FoundationDB `v5.1.7` Docker image based on Ubuntu `18.04`

## Build

```bash
git clone https://github.com/apple/foundationdb
docker build -t foundationdb:latest foundationdb/docker
```

## Usage

```bash
docker run -d \
  -e FDB_UID=$(id -u) \
  -e FDB_GID=$(id -g) \
  -v $(pwd)/etc:/etc/foundationdb \
  -v $(pwd)/log:/var/log/foundationdb \
  -v $(pwd)/data:/var/lib/foundationdb/data \
  -P \
  --init \
  foundationdb:latest
```
