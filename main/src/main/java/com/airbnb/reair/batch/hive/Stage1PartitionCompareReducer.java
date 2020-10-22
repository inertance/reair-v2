package com.airbnb.reair.batch.hive;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.ClusterFactory;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.primitives.TaskEstimate;
import com.airbnb.reair.incremental.primitives.TaskEstimator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer to process partition entities.
 *
 * <p>Table and partition entities are evenly distributed to reducers via shuffle. For partition
 * entities, the reducer will figure out the action to take. For table entities, the reducer will
 * pass them through to the next stage.
 */
public class Stage1PartitionCompareReducer extends Reducer<LongWritable, Text, Text, Text> {
  private static final Log LOG = LogFactory.getLog(Stage1PartitionCompareReducer.class);

  private static final DestinationObjectFactory destinationObjectFactory =
      new DestinationObjectFactory();

  private Configuration conf;
  private HiveMetastoreClient srcClient;
  private HiveMetastoreClient dstClient;
  private Cluster srcCluster;
  private Cluster dstCluster;
  private DirectoryCopier directoryCopier;
  private long count = 0;
  private TaskEstimator estimator;

  public Stage1PartitionCompareReducer() {
  }

  protected void setup(Context context) throws IOException, InterruptedException {
    try {
      this.conf = context.getConfiguration();

      ClusterFactory clusterFactory = MetastoreReplUtils.createClusterFactory(conf);

      this.srcCluster = clusterFactory.getSrcCluster();
      this.srcClient = this.srcCluster.getMetastoreClient();

      this.dstCluster = clusterFactory.getDestCluster();
      this.dstClient = this.dstCluster.getMetastoreClient();

      this.directoryCopier = clusterFactory.getDirectoryCopier();

      this.estimator = new TaskEstimator(conf,
          destinationObjectFactory,
          srcCluster,
          dstCluster,
          directoryCopier);
    } catch (HiveMetastoreException | ConfigurationException e) {
      throw new IOException(e);
    }
  }

  protected void reduce(LongWritable key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    for (Text value : values) {
      Pair<TaskEstimate, HiveObjectSpec> input =
          MetastoreReplicationJob.deseralizeJobResult(value.toString());
      TaskEstimate estimate = input.getLeft();
      HiveObjectSpec spec = input.getRight();
      String result = value.toString();
      String extra = "";

      try {
        if (estimate.getTaskType() == TaskEstimate.TaskType.CHECK_PARTITION) {
          // Table exists in source, but not in dest. It should copy the table.
          TaskEstimate newEstimate = estimator.analyze(spec);

          result = MetastoreReplicationJob.serializeJobResult(newEstimate, spec);
        }
      } catch (HiveMetastoreException e) {
        LOG.error(String.format("Hit exception during db:%s, tbl:%s, part:%s", spec.getDbName(),
              spec.getTableName(), spec.getPartitionName()), e);
        extra = String.format("exception in %s of mapper = %s", estimate.getTaskType().toString(),
            context.getTaskAttemptID().toString());
      }

      context.write(new Text(result), new Text(extra));
      ++this.count;
      if (this.count % 100 == 0) {
        LOG.info("Processed " + this.count + " entities");
      }
    }
  }

  protected void cleanup(Context context) throws IOException,
            InterruptedException {
    this.srcClient.close();
    this.dstClient.close();
  }
}
