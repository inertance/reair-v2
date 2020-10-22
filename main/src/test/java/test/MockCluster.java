package test;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.incremental.configuration.Cluster;

import org.apache.hadoop.fs.Path;

public class MockCluster implements Cluster {

  private String name;
  private HiveMetastoreClient client;
  private Path fsRoot;
  private Path tmpDir;

  /**
   * Constructs a mock cluster with static values.
   *
   * @param name name of the cluster
   * @param client the Hive metastore client to use
   * @param fsRoot the root of the warehouse directory associated with the supplied metastore
   * @param tmpDir the root of the directory to use for temporary files
   */
  public MockCluster(String name, HiveMetastoreClient client, Path fsRoot, Path tmpDir) {
    this.name = name;
    this.client = client;
    this.fsRoot = fsRoot;
    this.tmpDir = tmpDir;
  }

  @Override
  public HiveMetastoreClient getMetastoreClient() throws HiveMetastoreException {
    return client;
  }

  @Override
  public Path getFsRoot() {
    return fsRoot;
  }

  @Override
  public Path getTmpDir() {
    return tmpDir;
  }

  @Override
  public String getName() {
    return name;
  }
}
