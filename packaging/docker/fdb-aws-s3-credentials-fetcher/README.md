# fdb-aws-s3-credentials-fetcher

This script will fetch credentials to access AWS S3 and store them in a [FDB compatible format](https://apple.github.io/foundationdb/backups.html#blob-credential-files).
The intention of this script is to be long running. The adjacent Dockefile builds a container that can be used
as a sidecar that runs adjacent to an fdb pod; the sidekick keeps the s3 credentials up-to-date for the adjacent
running fdbservers to use making backups, bulkloads, etc., to s3.

This script will check every minute if the current credentials are valid for at least the next 5 minutes.
If so, it will sleep another 60 seconds. If not, the script will fetch new credentials.
