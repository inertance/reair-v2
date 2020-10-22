-- Holds the state of replication jobs
CREATE TABLE `replication_jobs` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` timestamp NULL DEFAULT '0000-00-00 00:00:00',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `operation` varchar(128) DEFAULT NULL,
  `status` varchar(128) DEFAULT NULL,
  `src_path` varchar(4000) DEFAULT NULL,
  `src_cluster` varchar(256) DEFAULT NULL,
  `src_db` varchar(1024) DEFAULT NULL,
  `src_table` varchar(1024) DEFAULT NULL,
  `src_partitions` mediumtext,
  `src_tldt` varchar(4000) DEFAULT NULL,
  `rename_to_db` varchar(1024) DEFAULT NULL,
  `rename_to_table` varchar(1024) DEFAULT NULL,
  `rename_to_partition` varchar(4000) DEFAULT NULL,
  `rename_to_path` varchar(4000) DEFAULT NULL,
  `extras` mediumtext,
  PRIMARY KEY (`id`),
  KEY `update_time_index` (`update_time`),
  KEY `src_cluster_index` (`src_cluster`(255)),
  KEY `src_db_index` (`src_db`(255)),
  KEY `src_table_index` (`src_table`(255))
);

-- Holds misc. key value pairs
CREATE TABLE `key_value` (
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `key_string` varchar(128) NOT NULL,
  `value_string` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`key_string`)
);
