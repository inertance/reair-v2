# Known Issues
## Incremental Replication
* Due to https://issues.apache.org/jira/browse/HIVE-12865, exchange partition commands will be replicated under limited conditions. Resolution is pending.
* Since the audit log hook writes the changes in a separate transaction from the Hive metastore, it's possible to miss updates if the client fails after the metastore write, but before hook execution. In practice, this is not an issue as failed Hive queries are re-run.
