export HADOOP_OPTS="-Dlog4j.configuration=file:///etc/hadoop/conf/log4j.properties"
export HADOOP_HEAPSIZE=8096
start=`date +%s%3N`
echo "start time is $start"
export HADOOP_USER_NAME=hive;hadoop jar airbnb-reair-main-1.0.0-all.jar com.airbnb.reair.batch.hive.MetastoreReplicationJob --config-files batch_without_partition_cfg.xml --table-list hdfs:///tmp/test/table_list_without_partition.txt
end=`date +%s%3N`
echo "stop time is $end"
cost=$((end-$start))
echo "cost time is: $cost"
