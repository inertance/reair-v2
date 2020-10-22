package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.batch.hdfs.ReplicationJob;
import com.airbnb.reair.batch.hive.MetastoreReplicationJob;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Unit test for batch replication.
 */
public class BatchReplicationTest extends MockClusterTest {

  /**
   * Sets up this class to have the right defaults for the batch replication test.
   *
   * @throws IOException if there's an error writing to files
   * @throws SQLException if there's an error querying the embedded DB
   */
  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
  }

  @Test
  public void testCopyNewTables() throws Exception {
    // Create an unpartitioned table in the source
    final HiveObjectSpec spec = new HiveObjectSpec("test", "test_table");
    final Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf,
        srcMetastore,
        spec,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    // Create a partitioned table in the source
    final HiveObjectSpec tableSpec = new HiveObjectSpec("test", "partitioned_table");
    final Table srcTable2 = ReplicationTestUtils.createPartitionedTable(conf,
        srcMetastore,
        tableSpec,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    // Create several partitions in the source table
    HiveObjectSpec partitionSpec1 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=1");
    HiveObjectSpec partitionSpec2 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=2");
    HiveObjectSpec partitionSpec3 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=3");

    final Partition srcPartition1 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec1);
    final Partition srcPartition2 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec2);
    final Partition srcPartition3 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec3);

    JobConf jobConf = new JobConf(conf);

    String[] args = {};
    jobConf.set(ConfigurationKeys.BATCH_JOB_OUTPUT_DIR,
        new Path(destCluster.getFsRoot(), "test_output").toString());
    jobConf.set(ConfigurationKeys.BATCH_JOB_CLUSTER_FACTORY_CLASS,
        MockClusterFactory.class.getName());

    ToolRunner.run(jobConf, new MetastoreReplicationJob(), args);

    assertTrue(ReplicationUtils.exists(destMetastore, spec));

    Table dstTable = destMetastore.getTable(spec.getDbName(), spec.getTableName());
    assertTrue(directoryCopier.equalDirs(new Path(srcTable.getSd().getLocation()),
          new Path(dstTable.getSd().getLocation())));

    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec1));
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec2));
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec3));

    Partition dstPartition1 = destMetastore.getPartition(partitionSpec1.getDbName(),
        partitionSpec1.getTableName(),
        partitionSpec1.getPartitionName());
    assertTrue(directoryCopier.equalDirs(new Path(srcPartition1.getSd().getLocation()),
          new Path(dstPartition1.getSd().getLocation())));

    Partition dstPartition2 = destMetastore.getPartition(partitionSpec2.getDbName(),
        partitionSpec2.getTableName(),
        partitionSpec2.getPartitionName());
    assertTrue(directoryCopier.equalDirs(new Path(srcPartition2.getSd().getLocation()),
          new Path(dstPartition2.getSd().getLocation())));

    Partition dstPartition3 = destMetastore.getPartition(partitionSpec3.getDbName(),
        partitionSpec3.getTableName(),
        partitionSpec3.getPartitionName());
    assertTrue(directoryCopier.equalDirs(new Path(srcPartition3.getSd().getLocation()),
          new Path(dstPartition3.getSd().getLocation())));

    ReplicationTestUtils.dropTable(srcMetastore, spec);
    ReplicationTestUtils.dropPartition(srcMetastore, partitionSpec2);

    assertEquals(0, ToolRunner.run(jobConf, new MetastoreReplicationJob(), args));

    assertTrue(!ReplicationUtils.exists(destMetastore, partitionSpec2));
  }


  @Test
  public void testHdfsCopy() throws Exception {
    // Create an unpartitioned table in the source
    HiveObjectSpec spec = new HiveObjectSpec("test", "test_table");
    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf,
        srcMetastore,
        spec,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    // Create a partitioned table in the source
    HiveObjectSpec tableSpec = new HiveObjectSpec("test", "partitioned_table");
    Table srcTable2 = ReplicationTestUtils.createPartitionedTable(conf,
        srcMetastore,
        tableSpec,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    // Create several partitions in the source table
    HiveObjectSpec partitionSpec1 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=1");
    HiveObjectSpec partitionSpec2 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=2");
    HiveObjectSpec partitionSpec3 = new HiveObjectSpec("test",
        "partitioned_table", "ds=1/hr=3");

    Partition srcPartition1 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec1);
    Partition srcPartition2 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec2);
    Partition srcPartition3 = ReplicationTestUtils.createPartition(conf,
        srcMetastore, partitionSpec3);

    JobConf jobConf = new JobConf(conf);

    String[] args = {"--" + ReplicationJob.SOURCE_DIRECTORY_ARG, srcWarehouseRoot.toString(),
        "--" + ReplicationJob.DESTINATION_DIRECTORY_ARG, destWarehouseRoot.toString(),
        "--" + ReplicationJob.LOG_DIRECTORY_ARG,
        new Path(destCluster.getFsRoot(), "log").toString(),
        "--" + ReplicationJob.TEMP_DIRECTORY_ARG, destCluster.getTmpDir().toString(),
        "--" + ReplicationJob.OPERATIONS_ARG, "a,d,u"};

    assertEquals(0, ToolRunner.run(jobConf, new ReplicationJob(), args));

    assertTrue(directoryCopier.equalDirs(srcWarehouseRoot, destWarehouseRoot));
  }
}
