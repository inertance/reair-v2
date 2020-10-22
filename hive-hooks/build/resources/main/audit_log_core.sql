# Stores core audit log stats for queries, e.g. the query id

CREATE TABLE `audit_log` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `query_id` varchar(256) DEFAULT NULL,
  `command_type` varchar(64) DEFAULT NULL,
  `command` mediumtext,
  `inputs` mediumtext,
  `outputs` mediumtext,
  `username` varchar(64) DEFAULT NULL,
  `ip` varchar(64) DEFAULT NULL,
  `extras` mediumtext,
  PRIMARY KEY (`id`),
  KEY `create_time_index` (`create_time`)
);
