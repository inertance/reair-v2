export HADOOP_OPTS="-Dlog4j.configuration=file:///root/private/tmp/reair/log4j.properties";
sudo -u hive hadoop jar  airbnb-reair-main-1.0.0-all.jar com.airbnb.reair.incremental.deploy.ReplicationLauncher --config-files /tmp/test/increment_config_file.xml
