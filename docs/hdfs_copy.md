# Large HDFS directory copy

## Overview

This is a tool for migrating HDFS data when `distcp` has issues copying a directory with a large number of files.

Depending on the options specified, it can:

* Copy files that exist on the source but not on the destination (add option)
* Copy files that exist on the source and on the destination but differ in file size (update option)
* Delete files that exist on the destination, but not the source (delete option)

Directories can be excluded from the copy by configuring the blacklist regex option. Paths (e.g. `/a/b/c`) matching the regex are not traversed.

The dry run mode only does a comparison between source and destination directories and outputs the operations that it would have done to the logging directory in text format. Please see the schema of the logging table below for details on the output.

## Usage

* Switch to the repo directory and build the JAR.

```
cd reair
gradlew shadowjar -p main -x test
```

* If Hive logging table does not exist, create it using [these commands](../main/src/main/resources/create_hdfs_copy_logging_tables.hql).

* CLI options:
```
  -source: source directory
  -destination: destination directory
  -temp-path: temporary directory where files will be copied to first
  -output-path: directory for logging data as produced by the MR job
  -operation: comma separated options for the copy: a(add), d(delete), u(update)
  -blacklist: skip directory names matching this regex
  -dry-run: don't execute the copy, but populate logging directory with data about planned operations
```

The following is an example invocation. Please replace with appropriate values before trying this out. Typically, the job should be run on the destination cluster.

```
export HADOOP_HEAPSIZE=8096
JOB_START_TIME="$(date +"%s")"

hadoop jar airbnb-reair-main-1.0.0-all.jar \
com.airbnb.reair.batch.hdfs.ReplicationJob \
-Dmapreduce.job.reduces=500 \
-Dmapreduce.map.memory.mb=8000 \
-Dmapreduce.map.java.opts="-Djava.net.preferIPv4Stack=true -Xmx7000m" \
-source hdfs://airfs-src/user/hive/warehouse \
-destination hdfs://airfs-dest/user/hive/warehouse \
-log hdfs://airfs-dest/user/replication/log/$JOB_START_TIME \
-temp hdfs://airfs-dest/tmp/replication/$JOB_START_TIME$
-blacklist ".*/tmp/.*" \
-operations a,u,d

# Load the log into a Hive table for easy viewing
hive -e "LOAD  DATA  INPATH  'hdfs://airfs-dest/user/replication/log/$JOB_START_TIME/stage2' OVERWRITE INTO TABLE hdfs_copy_results PARTITION (job_start_time = $JOB_START_TIME);"
```
