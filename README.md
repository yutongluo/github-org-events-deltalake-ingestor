# Github Org Events DeltaLake Ingestor

## Requirements
* spark-3.3.1-bin-hadoop3
* Java 17
* GitHub Application ID
* Azure Storage Blob created with ADLS Gen 2
* Service Principal with [assigned the Storage Blob Data Contributor role](https://learn.microsoft.com/en-us/azure/storage/blobs/assign-azure-role-data-access?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&tabs=portal)

## Running
1. Create a file in `src/main/resources/settings.conf` with the following
content
    ```
    token=<github-api-token>
    app_id=<github-application-id>
    account_name=<storage-account-name>
    directory_id=<directory_id> 
    password=U-l8Q~AcG~Fmy5uTklapyBYzRJkH-aszR68TzbJS
    ```
2. Run `mvn package`
3. To store locally (store table in local /tmp/ folder), execute with `$SPARK_HOME/bin/spark-submit --packages io.delta:delta-core_2.12:2.2.0 --class SparkIngestMain --master 'local[4]' ~/github-events-ingest/target/github-events-ingest-1.0-SNAPSHOT.jar -o microsoft`
4. To store in remote location specified in settings.conf, run with added `-e prod` option

> If spark-submit complaints about missing jars, add them manually to jars folder of your spark submit