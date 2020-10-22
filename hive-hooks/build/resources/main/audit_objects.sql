# Stores serialized objects for hive queries

CREATE TABLE `audit_objects` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `audit_log_id` bigint(20) NOT NULL,
  `category` varchar(64) DEFAULT NULL,
  `type` varchar(64) DEFAULT NULL,
  `name` varchar(4000) DEFAULT NULL,
  `serialized_object` mediumtext,
  PRIMARY KEY (`id`),
  KEY `create_time_index` (`create_time`),
  KEY `audit_log_id_index` (`audit_log_id`)
);
