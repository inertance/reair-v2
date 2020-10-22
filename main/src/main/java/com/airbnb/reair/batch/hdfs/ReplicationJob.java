package com.airbnb.reair.batch.hdfs;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;

import com.airbnb.reair.batch.BatchUtils;
import com.airbnb.reair.batch.SimpleFileStatus;
import com.airbnb.reair.common.FsUtils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nullable;

/**
 * A Map/Reduce job that copies HDFS files from one or more source directories to a destination
 * directory.
 * In case of conflict in sources, the source with largest timestamp value is copied.
 *
 * <p>See https://github.com/airbnb/reair/blob/master/docs/hdfs_copy.md for usage.
 *
 * <p>ReplicationJob was a tool that was developed to address some of the shortcomings of DistCp
 * when copying directories with a large number of files (e.g. /user/hive/warehouse). It was
 * included in the repo since it might be useful, but it's not directly used for Hive replication.
 *
 * <p>It can potentially replace DistCp in incremental replication, but since incremental
 * replication generally runs copies for shallow directories with a relatively small number of
 * files (e.g. /user/hive/warehouse/my_table/ds=2016-01-01), there isn't a strong need.
 *
 * <p>There are 2 Map-Reduce jobs in this job:
 *
 * <p>1. runDirectoryComparisonJob
 *
 * <p>1.1. job.setInputFormatClass(DirScanInputFormat.class) -
 *      take all source roots and destination root as inputs,
 *      do an initial glob on inputs to get initial dir list,
 *      then breadth-first search on the initial dir list until it reaches max_level and get enough
 *      directories (note that the search in each level is done in a multi-threaded way).
 *
 * <p>1.2. job.setMapperClass(ListFileMapper.class) -
 *      list the files in those dirs (and recursively on the leaf dirs)
 *
 * <p>1.3. job.setReducerClass(DirectoryCompareReducer.class)
 *      for the same file path name, use the newest one from all source roots to compare with the
 *      one in destination, generate the action, and write the action into the reducer output.
 *
 * <p>2. runSyncJob
 *
 * <p>2.1. job.setInputFormatClass(TextInputFormat.class);
 *
 * <p>2.2. job.setMapperClass(HdfsSyncMapper.class) -
 *      Redistribute all file actions based on hash of filenames.
 *
 * <p>2.3. job.setReducerClass(HdfsSyncReducer.class) -
 *      Take the action.  Note that only ADD and UPDATE are supported.
 *      TODO: DELETE needs to be added.
 */
public class ReplicationJob extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ReplicationJob.class);

  private static final String SRC_PATH_CONF = "replication.src.path";
  private static final String DST_PATH_CONF = "replication.dst.path";
  private static final String TMP_PATH_CONF = "replication.tmp.path";
  private static final String COMPARE_OPTION_CONF = "replication.compare.option";

  // Names of the command line argument (e.g. --source)
  public static final String SOURCE_DIRECTORY_ARG = "source";
  public static final String DESTINATION_DIRECTORY_ARG = "destination";
  public static final String TEMP_DIRECTORY_ARG = "temp";
  public static final String LOG_DIRECTORY_ARG = "log";
  public static final String OPERATIONS_ARG = "operations";
  public static final String BLACKLIST_ARG = "blacklist";
  public static final String DRY_RUN_ARG = "dry-run";

  public static final String DIRECTORY_BLACKLIST_REGEX = "replication.directory.blacklist";

  private enum Operation {
    ADD,
    DELETE,
    UPDATE;

    public static Operation getEnum(String value) {
      if (value.equals("a")) {
        return ADD;
      } else if (value.equals("d")) {
        return DELETE;
      } else if (value.equals("u")) {
        return UPDATE;
      }

      throw new RuntimeException("Invalid Operation");
    }
  }

  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  private static URI findRootUri(URI [] rootUris, Path path) {
    return Stream.of(rootUris).filter(uri -> !uri.relativize(path.toUri()).equals(path.toUri()))
            .findFirst().get();
  }

  public static class ListFileMapper extends Mapper<Text, Boolean, Text, FileStatus> {
    private String directoryBlackList;
    // Store root URI for sources and destination directory
    private URI [] rootUris;

    private void enumDirectories(FileSystem fs, URI rootUri, Path directory, boolean recursive,
        Mapper.Context context) throws IOException, InterruptedException {
      try {
        for (FileStatus status : fs.listStatus(directory, hiddenFileFilter)) {
          if (status.isDirectory()) {
            if (recursive) {
              if (directoryBlackList == null
                  || !status.getPath().getName().matches(directoryBlackList)) {
                enumDirectories(fs,rootUri, status.getPath(), recursive, context);
              }
            }
          } else {
            context.write(new Text(rootUri.relativize(directory.toUri()).getPath()),
                    new FileStatus(status));
          }
        }
        context.progress();
      } catch (FileNotFoundException e) {
        return;
      }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      this.rootUris = Stream.concat(
          Stream.of(context.getConfiguration().get(DST_PATH_CONF)),
          Stream.of(context.getConfiguration().get(SRC_PATH_CONF).split(","))).map(
            root -> new Path(root).toUri()).toArray(size -> new URI[size]
          );
      this.directoryBlackList = context.getConfiguration().get(DIRECTORY_BLACKLIST_REGEX);
    }

    @Override
    protected void map(Text key, Boolean value, Context context)
        throws IOException, InterruptedException {
      Path directory = new Path(key.toString());
      FileSystem fileSystem = directory.getFileSystem(context.getConfiguration());

      enumDirectories(fileSystem, findRootUri(rootUris, directory), directory, value, context);
      LOG.info(key.toString() + " processed.");
    }
  }

  private static Text generateValue(String action, SimpleFileStatus fileStatus) {
    ArrayList<String> fields = new ArrayList<>();

    fields.add(action);
    fields.add(fileStatus.getFullPath());
    fields.add(String.valueOf(fileStatus.getFileSize()));
    fields.add(String.valueOf(fileStatus.getModificationTime()));

    return new Text(Joiner.on("\t").useForNull("\\N").join(fields));
  }

  /**
   * Compare source1 + source2 with destination.
   */
  public static class DirectoryCompareReducer extends Reducer<Text, FileStatus, Text, Text> {
    private URI dstRoot;
    // Store root URI for sources and destination directory
    private URI [] rootUris;
    private Predicate<SimpleFileStatus> underDstRootPred;
    private EnumSet<Operation> operationSet;

    private SimpleFileStatus findSrcFileStatus(List<SimpleFileStatus> fileStatuses) {
      // pick copy source. The source is the one with largest timestamp value
      return Ordering.from(new Comparator<SimpleFileStatus>() {
        @Override
        public int compare(SimpleFileStatus o1, SimpleFileStatus o2) {
          return Long.compare(o1.getModificationTime(), o2.getModificationTime());
        }
      }).max(fileStatuses);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      this.dstRoot = new Path(context.getConfiguration().get(DST_PATH_CONF)).toUri();
      this.underDstRootPred = new Predicate<SimpleFileStatus>() {
        @Override
        public boolean apply(@Nullable SimpleFileStatus simpleFileStatus) {
          return !dstRoot.relativize(simpleFileStatus.getUri())
                  .equals(simpleFileStatus.getUri());
        }
      };

      this.operationSet = Sets.newEnumSet(
        Iterables.<String, Operation>transform(
          Arrays.asList(context.getConfiguration().get(COMPARE_OPTION_CONF, "a,d,u")
                  .split(",")),
          new Function<String, Operation>() {
            @Override
            public Operation apply(@Nullable String value) {
              return Operation.getEnum(value);
            }
          }),
        Operation.class);
      this.rootUris = Stream.concat(
              Stream.of(context.getConfiguration().get(DST_PATH_CONF)),
              Stream.of(context.getConfiguration().get(SRC_PATH_CONF).split(","))).map(
                  root -> new Path(root).toUri()).toArray(size -> new URI[size]
      );
    }

    @Override
    protected void reduce(Text key, Iterable<FileStatus> values, Context context)
        throws IOException, InterruptedException {
      ListMultimap<String, SimpleFileStatus> fileStatusHashMap = LinkedListMultimap.create();

      for (FileStatus fs : values) {
        SimpleFileStatus efs =
            new SimpleFileStatus(fs.getPath(), fs.getLen(), fs.getModificationTime());
        URI rootUris = findRootUri(this.rootUris, fs.getPath());
        fileStatusHashMap.put(rootUris.relativize(fs.getPath().toUri()).getPath(), efs);
      }

      for (String relativePath : fileStatusHashMap.keySet()) {
        List<SimpleFileStatus> fileStatuses = fileStatusHashMap.get(relativePath);
        ArrayList<SimpleFileStatus> srcFileStatus =
            Lists.newArrayList(Iterables.filter(fileStatuses,
                    Predicates.not(this.underDstRootPred)));
        ArrayList<SimpleFileStatus> dstFileStatus =
            Lists.newArrayList(Iterables.filter(fileStatuses, this.underDstRootPred));

        // If destination has file,
        if (dstFileStatus.size() > 0) {
          // we can only have one destination
          assert dstFileStatus.size() == 1;

          // There are two cases:
          //     update or delete.
          if (srcFileStatus.size() > 0) {
            // pick source first. The source is the one with largest timestamp value
            SimpleFileStatus finalSrcFileStatus = findSrcFileStatus(srcFileStatus);

            // If file size is different we need to copy
            if (finalSrcFileStatus.getFileSize() != dstFileStatus.get(0).getFileSize()) {
              if (operationSet.contains(Operation.UPDATE)) {
                context.write(new Text(relativePath),
                        generateValue(Operation.UPDATE.toString(), finalSrcFileStatus));
              }
            }
          } else {
            // source does not exist, then we need to delete if operation contains delete.
            if (operationSet.contains(Operation.DELETE)) {
              // 2. source does not exist it is delete
              context.write(new Text(relativePath),
                      generateValue(Operation.DELETE.toString(), dstFileStatus.get(0)));
            }
          }
        } else {
          // Destination does not exist. So we need to add the file if needed.
          if (operationSet.contains(Operation.ADD)) {
            // if no destination, then this is a new file.
            SimpleFileStatus src = findSrcFileStatus(srcFileStatus);
            context.write(new Text(relativePath),
                    generateValue(Operation.ADD.toString(), src));
          }
        }
      }
    }
  }

  // Mapper to rebalance files need to be copied.
  public static class HdfsSyncMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] fields = value.toString().split("\t");
      long hashValue = Hashing.murmur3_128()
          .hashLong(Long.valueOf(fields[3]).hashCode() * Long.valueOf(fields[4]).hashCode())
          .asLong();
      context.write(new LongWritable(hashValue), value);
    }
  }

  public static class HdfsSyncReducer extends Reducer<LongWritable, Text, Text, Text> {
    private String dstRoot;
    private Path tmpDirPath;
    private long copiedSize = 0;

    enum CopyStatus {
      COPIED,
      SKIPPED
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      this.dstRoot = context.getConfiguration().get(DST_PATH_CONF);
      this.tmpDirPath = new Path(context.getConfiguration().get(TMP_PATH_CONF));
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        String[] fields = value.toString().split("\t");
        Operation operation = Operation.valueOf(fields[1]);

        // We only support add operation for now.
        if (operation == Operation.ADD || operation == Operation.UPDATE) {
          SimpleFileStatus fileStatus =
              new SimpleFileStatus(fields[2], Long.valueOf(fields[3]), Long.valueOf(fields[4]));
          Path dstFile = new Path(dstRoot, fields[0]);

          FileSystem srcFs = (new Path(fileStatus.getFullPath()))
                  .getFileSystem(context.getConfiguration());
          FileSystem dstFs = dstFile.getFileSystem(context.getConfiguration());
          String copyError =
              BatchUtils.doCopyFileAction(context.getConfiguration(), fileStatus,
                  srcFs, dstFile.getParent().toString(),
                  dstFs, tmpDirPath, context, fields[1].equals("update"),
                  context.getTaskAttemptID().toString());

          if (copyError == null) {
            context.write(new Text(fields[0]),
                    generateValue(CopyStatus.COPIED.toString(), fileStatus));
          } else {
            context.write(new Text(fields[0]),
                    generateValue(CopyStatus.SKIPPED.toString(), fileStatus));
          }
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      LOG.info("Total bytes copied = " + copiedSize);
    }
  }

  /**
   * Print usage information to provided OutputStream.
   *
   * @param options Command-line options to be part of usage.
   * @param out OutputStream to which to write the usage information.
   */
  public static void printUsage(final Options options, final OutputStream out) {
    final PrintWriter writer = new PrintWriter(out);
    final HelpFormatter usageFormatter = new HelpFormatter();
    usageFormatter.printUsage(writer, 80,
        "Usage: hadoop jar <jar name> " + ReplicationJob.class.getName(), options);
    writer.flush();
  }

  /**
   * Construct and provide Options.
   *
   * @return Options expected from command-line of GNU form.
   */
  @SuppressWarnings("static-access")
  public static Options constructOptions() {
    Options options = new Options();

    options.addOption(OptionBuilder.withLongOpt(SOURCE_DIRECTORY_ARG)
            .withDescription(
                    "Comma separated list of source directories")
            .hasArg()
            .withArgName("S")
            .create());

    options.addOption(OptionBuilder.withLongOpt(DESTINATION_DIRECTORY_ARG)
            .withDescription("Copy destination directory")
            .hasArg()
            .withArgName("D")
            .create());

    options.addOption(OptionBuilder.withLongOpt(TEMP_DIRECTORY_ARG)
        .withDescription("Copy temporary directory path")
        .hasArg()
        .withArgName("T")
        .create());

    options.addOption(OptionBuilder.withLongOpt(LOG_DIRECTORY_ARG)
            .withDescription("Job logging output path")
            .hasArg()
            .withArgName("O")
            .create());

    options.addOption(OptionBuilder.withLongOpt(OPERATIONS_ARG)
            .withDescription("checking options: comma separated option"
                    + " including a(add), d(delete), u(update)")
            .hasArg()
            .withArgName("P")
            .create());

    options.addOption(OptionBuilder.withLongOpt(BLACKLIST_ARG)
            .withDescription("Directory blacklist regex")
            .hasArg()
            .withArgName("B")
            .create());

    options.addOption(OptionBuilder.withLongOpt(DRY_RUN_ARG)
            .withDescription("Dry run only")
            .create());

    return options;
  }

  /**
   * Method to run HDFS copy job.
   *  1. Parse program args.
   *  2. Run two MR jobs in sequence.
   *
   * @param args program arguments
   * @return 0 on success, 1 on failure
   *
   * @throws Exception IOException, InterruptedException, ClassNotFoundException
   */
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    final CommandLineParser cmdLineParser = new BasicParser();

    final Options options = constructOptions();
    CommandLine commandLine;

    try {
      commandLine = cmdLineParser.parse(options, args);
    } catch (ParseException e) {
      LOG.error("Encountered exception while parsing using GnuParser: ", e);
      printUsage(options, System.out);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
      return 1;
    }


    if (!commandLine.hasOption(SOURCE_DIRECTORY_ARG)
        || !commandLine.hasOption(DESTINATION_DIRECTORY_ARG)) {
      printUsage(options, System.out);
      return 1;
    }

    if (!commandLine.hasOption(LOG_DIRECTORY_ARG)) {
      printUsage(options, System.out);
      return 1;
    }

    boolean dryRun = commandLine.hasOption(DRY_RUN_ARG);
    Path srcDir = new Path(commandLine.getOptionValue(SOURCE_DIRECTORY_ARG));
    Path destDir = new Path(commandLine.getOptionValue(DESTINATION_DIRECTORY_ARG));
    String operationsStr = commandLine.getOptionValue(OPERATIONS_ARG);
    String tmpDirStr = commandLine.getOptionValue(TEMP_DIRECTORY_ARG);
    String blacklistRegex = commandLine.getOptionValue(BLACKLIST_ARG);

    if (blacklistRegex != null) {
      getConf().set(DIRECTORY_BLACKLIST_REGEX, blacklistRegex);
      LOG.info("Blacklist: " + blacklistRegex);
    }

    if (!dryRun && tmpDirStr == null) {
      LOG.error("Temporary directory must be specified");
      return -1;
    }

    // Disable speculative execution since neither mapper nor reducer handles this properly.
    if (this.getConf().getBoolean(MRJobConfig.MAP_SPECULATIVE, true)) {
      LOG.warn("Turning off speculative mappers in configuration");
      getConf().set(MRJobConfig.MAP_SPECULATIVE, "false");
    }
    if (this.getConf().getBoolean(MRJobConfig.REDUCE_SPECULATIVE, true)) {
      LOG.warn("Turning off speculative reducers in configuration");
      getConf().set(MRJobConfig.REDUCE_SPECULATIVE, "false");
    }

    Path logPath = new Path(commandLine.getOptionValue(LOG_DIRECTORY_ARG));
    // If the log directory exists and it is not empty, throw an error
    FileSystem fs = logPath.getFileSystem(getConf());
    if (!fs.exists(logPath)) {
      LOG.info("Creating " + logPath);
    } else if (FsUtils.getSize(getConf(), logPath, Optional.empty()) != 0) {
      LOG.error("Log directory already exists and is not empty: " + logPath);
      return -1;
    }

    Path stage1LogDir = new Path(logPath, "stage1");
    Path stage2LogDir = new Path(logPath, "stage2");

    if (dryRun) {
      LOG.info("Starting stage 1 with log directory " + stage1LogDir);
      return runDirectoryComparisonJob(srcDir,
          destDir,
          stage1LogDir,
          operationsStr);
    } else {
      Path tmpDir = new Path(tmpDirStr);

      // Verify that destination directory exists
      if (!FsUtils.dirExists(getConf(), destDir)) {
        LOG.warn("Destination directory does not exist. Creating " + destDir);
        FileSystem destFs = destDir.getFileSystem(getConf());
        fs.mkdirs(destDir);
      }

      LOG.info("Starting stage 1 with log directory " + stage1LogDir);
      if (runDirectoryComparisonJob(srcDir,
          destDir,
          stage1LogDir,
          operationsStr) == 0) {

        LOG.info("Starting stage 2 with log directory " + stage2LogDir);
        return runSyncJob(srcDir,
                destDir,
                tmpDir,
                stage1LogDir,
                stage2LogDir);
      } else {
        return -1;
      }
    }
  }

  private int runDirectoryComparisonJob(Path source, Path destination, Path output,
                                        String compareOption)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(getConf(), "Directory Comparison Job");
    job.setJarByClass(getClass());

    job.setInputFormatClass(DirScanInputFormat.class);
    job.setMapperClass(ListFileMapper.class);

    job.setReducerClass(DirectoryCompareReducer.class);

    // last directory is destination, all other directories are source directories
    job.getConfiguration().set(SRC_PATH_CONF, source.toString());
    job.getConfiguration().set(DST_PATH_CONF, destination.toString());
    job.getConfiguration().set(FileInputFormat.INPUT_DIR, Joiner.on(",").join(source, destination));
    job.getConfiguration().set(COMPARE_OPTION_CONF, compareOption);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FileStatus.class);

    FileOutputFormat.setOutputPath(job, output);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  private int runSyncJob(Path source, Path destination, Path tmpDir, Path input,
                         Path output)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(getConf(), "HDFS Sync job");
    job.setJarByClass(getClass());

    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(HdfsSyncMapper.class);
    job.setReducerClass(HdfsSyncReducer.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    job.getConfiguration().set(SRC_PATH_CONF, source.toString());
    job.getConfiguration().set(DST_PATH_CONF, destination.toString());
    job.getConfiguration().set(TMP_PATH_CONF, tmpDir.toString());

    FileInputFormat.setInputPaths(job, input);
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.setMaxInputSplitSize(job,
            this.getConf().getLong( FileInputFormat.SPLIT_MAXSIZE, 60000L));
    FileOutputFormat.setOutputPath(job, new Path(output.toString()));
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ReplicationJob(), args);
    System.exit(res);
  }
}
