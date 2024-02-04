# Set up the Azure Backup Testing Environment

Make sure we built FDB with `-DBUILD_AZURE_BACKUP=ON`

# Test

If you run _BackupToBlob_ and _RestoreFromBlob_ workloads with the parameter _backupURL_ starts with `azure://`,
the workload will backup to and restore from the azure blob storage.
For example, _BackupAzureBlobCorrectness.toml_

## Url format

The code now supports the following style urls:

- `azure://<account_name>.blob.core.windows.net/<container_name>` (The formal url format for the blob service provided by the azure storage account)
- `azure://<ip|hostname>:<port>/<account_name>/<container_name>` (Directly providing the endpoint address for the blob service, usually for local testing)

## Local test environment 

We need to use the _Azurite_ to simulate an Azure blob service locally.
Please follow the [tutorial](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub) to start your service locally.

For example,
```
docker run -p 10000:10000 -v `pwd`:<path> -w <path> mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --oauth basic --cert ./<...>.pem --key ./<...>.key.pem --debug ./<log_file_path>
```

### Notice

- To use uses _https_, we need to provide the certificates via `--cert` and `--key`
    The detailed [tutorial](https://github.com/Azure/Azurite/blob/main/README.md#https-setup) to setup HTTPS.  (We tested with the `mkcert` method)
- To use Azure SDKs, we need to pass `--oauth basic` option
- Please take a look at the [difference](https://github.com/Azure/Azurite/blob/main/README.md#differences-between-azurite-and-azure-storage) between Azurite and Azure Storage
