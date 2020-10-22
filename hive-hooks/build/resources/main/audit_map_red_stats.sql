# Stores mapreduce stats for hive queries
# Even though the "counters" column is JSON, store it as text since Hive may
# need to import this table and it does not support the JSON type
CREATE TABLE `mapred_stats` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `audit_log_id` bigint(20) NOT NULL,
  `stage` varchar(256) NOT NULL,
  `mappers` bigint(20) NOT NULL,
  `reducers` bigint(20) NOT NULL,
  `cpu_time` bigint(20) NOT NULL,
  `counters` text(240000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `create_time_index` (`create_time`),
  KEY `audit_log_id_index` (`audit_log_id`)
);
