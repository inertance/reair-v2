# Frequently Asked Questions

### How do I use these tools to migrate a warehouse?

One approach to migrating a warehouse is to use batch replication to get an initial copy of the tables in the original warehouse into the new warehouse. Once the initial copy is done, incremental replication can be used to replicate changes from a point shortly before batch replication was kicked off. Then, incremental replication keeps both clusters in sync until the cutover date.

### How do I run against different versions of Hadoop / Hive?

As shipped, the default configuration will work with most Hive and Hadoop v2 deployments because of the generally backward-compatible nature of the API calls used in this project. We have not been able to test against a variety of different versions, but it's possible to modify `build.gradle` and specify different Hadoop and Hive versions to produce more appropriate binaries if you encounter issues.

### For idempotent file copy operations, how is file equality determined?

To provide a fast check of equality, files are considered equal if the sizes and modified times match between the source and destination warehouses. Files that are considered equal will not be re-copied, and this behavior may not be suitable for all applications.

### How do I filter out specific tables from getting replicated?

Both batch and incremental replication provide a blacklist mechanism. For both, please see the example configuration templates for the appropriate configuration variable for blacklisting entries.

### How do tables on S3 get replicated?

For tables and partitions where the location is on S3, the metadata for the tables is copied over to the destination warehouse. Be aware that replicated S3-backed tables should be created as external tables, or there could be issues with Hive operations (e.g. `DROP TABLE`) causing inconsistencies.

## Batch Replication

### What kind of consistency guarantees does batch replication provide while it's running?

While batch replication is running, there are no consistency guarantees on the destination warehouse. Because files are copied directly to destination directories, it's possible to observe a partially complete data directory. Once batch replication finishes successfully, it guarantees that if a table exists in the metastore, it is consistent with the state of the source table at some point after batch replication was kicked off. In general, please wait for batch replication to finish running before running any queries on the destination warehouse.

## Incremental Replication

### What kind of consistency guarantees	does incremental replication provide while it's running?

Overall, incremental replication provides eventual consistency. Increment replication also guarantees that data directories never contain partial data. While incremental replication is running and there are updates to a table on the source warehouse, tables on the destination warehouse will either contain old data, new data, but never a partial result. These are the same semantics that Hive provides when overwriting a table with a query. In addition, incremental replication guarantees that data for a table will be copied before the before the metadata, so if a table is present in the metastore, the table can be queried.

### Are there any issues with restarting the incremental replication process from a previous point in the audit log?

Since incremental replication is idempotent, it is safe to restart from a previous point in the audit log. Files that have been copied will not be copied again, so recovery should be relatively fast.