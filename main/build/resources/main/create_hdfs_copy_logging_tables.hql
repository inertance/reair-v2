-- HDFS copy job output table.
CREATE TABLE IF NOT EXISTS hdfs_copy_results(
      dst_path string,  -- destination path
      action string,    -- action, add, update, delete
      src_path string,  -- source path
      size bigint,      -- size
      ts bigint)        -- file timestamp
PARTITIONED BY (
      job_start_time bigint)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
