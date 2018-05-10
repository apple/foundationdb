# FoundationDB Docker image

FoundationDB `v5.1.7` Docker image based on Ubuntu `18.04`

## Build

```bash
git clone https://github.com/apple/foundationdb
docker build -t foundationdb:latest foundationdb/docker
```

## Usage

```bash
docker run -d -v data:/data -p 4500:4500 foundationdb:latest
```
