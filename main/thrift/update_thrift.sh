set -x
thrift -r --gen java --gen rb reair.thrift && \
mkdir -p ../src/main/java/com/airbnb/di/hive/replication/thrift && \
cp ./gen-java/com/airbnb/di/hive/replication/thrift/* ../src/main/java/com/airbnb/di/hive/replication/thrift
