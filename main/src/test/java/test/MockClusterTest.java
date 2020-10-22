package test;

import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.configuration.ObjectConflictHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.sql.SQLException;

public abstract class MockClusterTest {

  private static final Log LOG = LogFactory.getLog(MockClusterTest.class);

  protected static MockHiveMetastoreClient srcMetastore;
  protected static MockHiveMetastoreClient destMetastore;

  protected static YarnConfiguration conf;
  protected static MiniYARNCluster miniCluster;

  protected static Cluster srcCluster;
  protected static Cluster destCluster;

  protected static DirectoryCopier directoryCopier;

  protected static ObjectConflictHandler conflictHandler;
  protected static DestinationObjectFactory destinationObjectFactory;

  // Temporary directories on the local filesystem that we'll treat as the
  // source and destination filesystems
  @Rule
  public TemporaryFolder srcLocalTmp = new TemporaryFolder();
  @Rule
  public TemporaryFolder destLocalTmp = new TemporaryFolder();

  protected Path srcWarehouseRoot;
  protected Path destWarehouseRoot;

  /**
   * Sets up this class for use in unit testing. It spins up the YARN mini-cluster and also sets up
   * various default classes.
   *
   * @throws IOException if there's an error accessing the local filesystem
   * @throws SQLException if there's an error querying the embedded DB
   */
  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
    miniCluster = new MiniYARNCluster("test", 1, 1, 1);
    miniCluster.init(conf);
    miniCluster.start();

    conflictHandler = new ObjectConflictHandler();
    conflictHandler.setConf(conf);

    destinationObjectFactory = new DestinationObjectFactory();
    destinationObjectFactory.setConf(conf);
  }

  /**
   * Setup to do for each unit test.
   *
   * @throws IOException if there's an error accessing the local file system
   */
  @Before
  public void setUp() throws IOException {
    srcMetastore = new MockHiveMetastoreClient();
    destMetastore = new MockHiveMetastoreClient();

    srcLocalTmp.create();
    destLocalTmp.create();

    final Path srcFsRoot = new Path("file://" + srcLocalTmp.getRoot().getAbsolutePath());
    final Path destFsRoot = new Path("file://" + destLocalTmp.getRoot().getAbsolutePath());

    srcWarehouseRoot = new Path(makeFileUri(srcLocalTmp), "warehouse");
    destWarehouseRoot = new Path(makeFileUri(destLocalTmp), "warehouse");

    srcWarehouseRoot.getFileSystem(conf).mkdirs(srcWarehouseRoot);
    destWarehouseRoot.getFileSystem(conf).mkdirs(destWarehouseRoot);

    LOG.info(String.format("src root: %s, dest root: %s", srcWarehouseRoot, destWarehouseRoot));

    final Path srcTmp = new Path(makeFileUri(this.srcLocalTmp), "tmp");
    final Path destTmp = new Path(makeFileUri(this.destLocalTmp), "tmp");

    srcCluster = new MockCluster("src_cluster", srcMetastore, srcFsRoot, srcTmp);
    destCluster = new MockCluster("dest_cluster", destMetastore, destFsRoot, destTmp);

    // Disable checking of modified times as the local filesystem does not
    // support this
    directoryCopier = new DirectoryCopier(conf, destCluster.getTmpDir(), false);
  }

  @AfterClass
  public static void tearDownClass() {
    miniCluster.stop();
  }

  protected static Path makeFileUri(TemporaryFolder directory) {
    return new Path("file://" + directory.getRoot().getAbsolutePath());
  }
}
