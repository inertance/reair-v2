package com.airbnb.reair.utils;

import com.google.common.collect.Lists;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveParameterKeys;
import com.airbnb.reair.common.PathBuilder;
import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.db.EmbeddedMySqlDb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for running replication tests.
 */
public class ReplicationTestUtils {

  private static final Log LOG = LogFactory.getLog(
      ReplicationTestUtils.class);


  private static Path getPathForHiveObject(Path warehouseRoot,
      HiveObjectSpec spec) {
    PathBuilder pb = new PathBuilder(warehouseRoot);
    pb.add(spec.getDbName());
    pb.add(spec.getTableName());
    if (spec.isPartition()) {
      pb.add(spec.getPartitionName());
    }
    return pb.toPath();
  }

  private static void createSomeTextFiles(Configuration conf, Path directory)
    throws IOException {
    createTextFile(conf, directory, "file1.txt", "foobar");
    createTextFile(conf, directory, "file2.txt", "123");
  }

  /**
   * Creates the specified text file using Hadoop API's.
   *
   * @param conf configuration object
   * @param directory directory where to create some files
   * @param filename TODO
   * @param contents TODO
   * @throws IOException TODO
   */
  public static void createTextFile(Configuration conf,
      Path directory,
      String filename,
      String contents)
    throws IOException {
    Path filePath = new Path(directory, filename);
    FileSystem fs = FileSystem.get(filePath.toUri(), conf);

    FSDataOutputStream file1OutputStream = fs.create(filePath);
    file1OutputStream.writeBytes(contents);
    file1OutputStream.close();
  }


  /**
   * Creates an unpartitioned table with some dummy files.
   *
   * @param conf TODO
   * @param ms TODO
   * @param tableSpec TODO
   * @param tableType TODO
   * @param warehouseRoot TODO
   * @return TODO
   * @throws IOException TODO
   * @throws HiveMetastoreException TODO
   */
  public static Table createUnpartitionedTable(Configuration conf,
      HiveMetastoreClient ms,
      HiveObjectSpec tableSpec,
      TableType tableType,
      Path warehouseRoot)
    throws IOException, HiveMetastoreException {

    // Set up the basic properties of the table
    Table table = new Table();
    table.setDbName(tableSpec.getDbName());
    table.setTableName(tableSpec.getTableName());
    Map<String, String> parameters = new HashMap<>();
    parameters.put(HiveParameterKeys.TLDT, Long.toString(
          System.currentTimeMillis()));
    table.setParameters(parameters);
    table.setPartitionKeys(new ArrayList<>());
    table.setTableType(tableType.toString());

    // Setup the columns and the storage descriptor
    StorageDescriptor sd = new StorageDescriptor();
    // Set the schema for the table
    List<FieldSchema> columns = new ArrayList<>();
    columns.add(new FieldSchema("key", "string",
          "my comment"));
    sd.setCols(columns);

    if (tableType == TableType.MANAGED_TABLE
        || tableType == TableType.EXTERNAL_TABLE) {
      Path tableLocation = getPathForHiveObject(warehouseRoot, tableSpec);
      sd.setLocation(tableLocation.toString());
      // Make some fake files
      createSomeTextFiles(conf, tableLocation);
    } else if (tableType == TableType.VIRTUAL_VIEW) {
      table.setTableType(TableType.VIRTUAL_VIEW.toString());
    }
    table.setSd(sd);

    // Create DB for table if one does not exist
    if (!ms.existsDb(table.getDbName())) {
      ms.createDatabase(new Database(table.getDbName(), null, null, null));
    }
    ms.createTable(table);

    return table;
  }

  /**
   * Creates a table that is partitioned on ds and hr.
   *
   * @param conf TODO
   * @param ms TODO
   * @param tableSpec TODO
   * @param tableType TODO
   * @param warehouseRoot TODO
   * @return TODO
   * @throws IOException TODO
   * @throws HiveMetastoreException TODO
   */
  public static Table createPartitionedTable(Configuration conf,
      HiveMetastoreClient ms,
      HiveObjectSpec tableSpec,
      TableType tableType,
      Path warehouseRoot)
    throws IOException, HiveMetastoreException {
    Path tableLocation = getPathForHiveObject(warehouseRoot, tableSpec);

    // Set up the basic properties of the table
    Table table = new Table();
    table.setDbName(tableSpec.getDbName());
    table.setTableName(tableSpec.getTableName());
    Map<String, String> parameters = new HashMap<>();
    parameters.put(HiveParameterKeys.TLDT, Long.toString(
          System.currentTimeMillis()));
    table.setParameters(parameters);
    table.setTableType(tableType.toString());

    // Set up the partitioning scheme
    List<FieldSchema> partitionCols = new ArrayList<>();
    partitionCols.add(new FieldSchema("ds", "string", "my ds comment"));
    partitionCols.add(new FieldSchema("hr", "string", "my hr comment"));
    table.setPartitionKeys(partitionCols);

    // Setup the columns and the storage descriptor
    StorageDescriptor sd = new StorageDescriptor();
    // Set the schema for the table
    List<FieldSchema> columns = new ArrayList<>();
    columns.add(new FieldSchema("key", "string",
          "my comment"));
    sd.setCols(columns);
    if (tableType == TableType.MANAGED_TABLE
        || tableType == TableType.EXTERNAL_TABLE) {
      sd.setLocation(tableLocation.toString());
    }

    sd.setSerdeInfo(new SerDeInfo("LazySimpleSerde",
          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
          new HashMap<>()));
    table.setSd(sd);

    // Create DB for table if one does not exist
    if (!ms.existsDb(table.getDbName())) {
      ms.createDatabase(new Database(table.getDbName(), null, null, null));
    }

    ms.createTable(table);

    return table;
  }

  /**
   * Creates a partition in a table with some dummy files.
   *
   * @param conf TODO
   * @param ms TODO
   * @param partitionSpec TODO
   * @return TODO
   * @throws IOException TODO
   * @throws HiveMetastoreException TODO
   */
  public static Partition createPartition(Configuration conf,
      HiveMetastoreClient ms,
      HiveObjectSpec partitionSpec)
    throws IOException, HiveMetastoreException {

    HiveObjectSpec tableSpec = partitionSpec.getTableSpec();
    if (! ms.existsTable(tableSpec.getDbName(),
          tableSpec.getTableName())) {
      throw new HiveMetastoreException("Missing table " + tableSpec);
    }
    Table table = ms.getTable(tableSpec.getDbName(), tableSpec.getTableName());

    Partition partition = new Partition();
    partition.setDbName(partitionSpec.getDbName());
    partition.setTableName(partitionSpec.getTableName());

    Map<String, String> partitionKeyValues = ms.partitionNameToMap(
        partitionSpec.getPartitionName());
    partition.setValues(Lists.newArrayList(partitionKeyValues.values()));
    StorageDescriptor psd = new StorageDescriptor(table.getSd());
    TableType tableType = TableType.valueOf(table.getTableType());
    if (tableType.equals(TableType.MANAGED_TABLE)
        || tableType.equals(TableType.EXTERNAL_TABLE)) {
      // Make the location for the partition to be in a subdirectory of the
      // table location. String concatenation here is not great.
      String partitionLocation = table.getSd().getLocation() + "/"
          + partitionSpec.getPartitionName();
      psd.setLocation(partitionLocation);
      createSomeTextFiles(conf, new Path(partitionLocation));
    }

    // Set the serde info as it can cause an NPE otherwise when creating
    // ql Partition objects.
    psd.setSerdeInfo(new SerDeInfo("LazySimpleSerde",
          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
          new HashMap<>()));
    partition.setSd(psd);

    Map<String, String> parameters = new HashMap<>();
    parameters.put(HiveParameterKeys.TLDT, Long.toString(
          System.currentTimeMillis()));
    partition.setParameters(parameters);

    ms.addPartition(partition);
    return partition;
  }

  /**
   * TODO.
   *
   * @param ms TODO
   * @param objectSpec TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public static void updateModifiedTime(HiveMetastoreClient ms,
      HiveObjectSpec objectSpec)
    throws HiveMetastoreException {
    if (objectSpec.isPartition()) {
      Partition partition = ms.getPartition(objectSpec.getDbName(),
          objectSpec.getTableName(), objectSpec.getPartitionName());
      partition.getParameters().put(HiveParameterKeys.TLDT,
          Long.toString(System.currentTimeMillis()));
    } else {
      Table table = ms.getTable(objectSpec.getDbName(),
          objectSpec.getTableName());
      table.getParameters().put(HiveParameterKeys.TLDT,
          Long.toString(System.currentTimeMillis()));
    }

  }

  /**
   * TODO.
   *
   * @param ms TODO
   * @param objectSpec TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public static String getModifiedTime(HiveMetastoreClient ms,
      HiveObjectSpec objectSpec)
    throws HiveMetastoreException {
    if (objectSpec.isPartition()) {
      Partition partition = ms.getPartition(objectSpec.getDbName(),
          objectSpec.getTableName(), objectSpec.getPartitionName());
      return partition.getParameters().get(HiveParameterKeys.TLDT);
    } else {
      Table table = ms.getTable(objectSpec.getDbName(),
          objectSpec.getTableName());
      return table.getParameters().get(HiveParameterKeys.TLDT);
    }
  }

  /**
   * TODO.
   *
   * @param jdbcUrl TODO
   * @param username TODO
   * @param password TODO
   * @param tableName TODO
   * @param columnNames TODO
   * @return TODO
   *
   * @throws ClassNotFoundException TODO
   * @throws SQLException TODO
   */
  public static List<String> getRow(String jdbcUrl, String username,
      String password,
      String tableName,
      List<String> columnNames)
    throws ClassNotFoundException, SQLException {
    return getRow(jdbcUrl, username, password, tableName, columnNames, null);
  }

  /**
   * TODO.
   *
   * @param jdbcUrl TODO
   * @param username TODO
   * @param password TODO
   * @param tableName TODO
   * @param columnNames TODO
   * @param whereClause TODO
   * @return TODO
   *
   * @throws ClassNotFoundException TODO
   * @throws SQLException TODO
   */
  public static List<String> getRow(String jdbcUrl,
      String username,
      String password,
      String tableName,
      List<String> columnNames,
      String whereClause)
    throws ClassNotFoundException, SQLException {
    StringBuilder qb = new StringBuilder();

    List<String> columnExpressions = new ArrayList<>();
    for (String columnName : columnNames) {
      columnExpressions.add(String.format("CAST(%s AS CHAR)", columnName));
    }


    qb.append("SELECT ");
    qb.append(StringUtils.join(", ", columnExpressions));

    qb.append(" FROM ");
    qb.append(tableName);
    if (whereClause != null) {
      qb.append(" WHERE ");
      qb.append(whereClause);
    }

    LOG.debug("Running query " + qb.toString());

    Class.forName("com.mysql.jdbc.Driver");
    Connection connection = DriverManager.getConnection(jdbcUrl, username,
        password);

    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery(qb.toString());

    List<String> row = null;

    if (rs.next()) {
      row = new ArrayList<>();
      for (int i = 1; i <= columnNames.size(); i++) {
        row.add(rs.getString(i));
      }
    }
    connection.close();
    return row;
  }

  /**
   * TODO.
   *
   * @param dbConnectionFactory TODO
   * @param dbName TODO
   * @param tableName TODO
   * @param columnNames TODO
   * @param whereClause TODO
   * @return TODO
   *
   * @throws ClassNotFoundException TODO
   * @throws SQLException TODO
   */
  public static List<String> getRow(DbConnectionFactory dbConnectionFactory,
      String dbName,
      String tableName,
      List<String> columnNames,
      String whereClause)
    throws ClassNotFoundException, SQLException {
    StringBuilder qb = new StringBuilder();

    List<String> columnExpressions = new ArrayList<>();
    for (String columnName : columnNames) {
      columnExpressions.add(String.format("CAST(%s AS CHAR)", columnName));
    }


    qb.append("SELECT ");
    qb.append(StringUtils.join(", ", columnExpressions));

    qb.append(" FROM ");
    qb.append(tableName);
    if (whereClause != null) {
      qb.append(" WHERE ");
      qb.append(whereClause);
    }

    LOG.debug("Running query " + qb.toString());

    Class.forName("com.mysql.jdbc.Driver");
    Connection connection = dbConnectionFactory.getConnection();
    connection.setCatalog(dbName);

    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery(qb.toString());

    List<String> row = null;

    if (rs.next()) {
      row = new ArrayList<>();
      for (int i = 1; i <= columnNames.size(); i++) {
        row.add(rs.getString(i));
      }
    }
    connection.close();
    return row;
  }

  public static String getJdbcUrl(EmbeddedMySqlDb db) {
    return String.format("jdbc:mysql://%s:%s/",
        db.getHost(), db.getPort());
  }

  public static String getJdbcUrl(EmbeddedMySqlDb db, String dbName) {
    return String.format("jdbc:mysql://%s:%s/%s",
        db.getHost(), db.getPort(), dbName);
  }

  /**
   * TODO.
   *
   * @param connectionFactory TODO
   * @param dbName TODO
   * @throws SQLException TODO
   */
  public static void dropDatabase(DbConnectionFactory connectionFactory,
      String dbName) throws SQLException {
    Connection connection = connectionFactory.getConnection();
    String sql = String.format("DROP DATABASE IF EXISTS %s", dbName);
    Statement statement = connection.createStatement();
    try {
      statement.execute(sql);
    } finally {
      statement.close();
      connection.close();
    }
  }

  /**
   * Drops all tables from the given Hive DB.
   *
   * @param ms TODO
   * @param dbName TODO
   * @throws HiveMetastoreException TODO
   */
  public static void dropTables(HiveMetastoreClient ms, String dbName)
    throws HiveMetastoreException {
    for (String tableName : ms.getTables(dbName, "*")) {
      ms.dropTable(dbName, tableName, true);
    }
  }

  /**
   * Drops a table from the given Hive DB.
   *
   * @param ms TODO
   * @param spec TODO
   * @throws HiveMetastoreException TODO
   */
  public static void dropTable(HiveMetastoreClient ms, HiveObjectSpec spec)
    throws HiveMetastoreException {
    ms.dropTable(spec.getDbName(), spec.getTableName(), true);
  }

  /**
   * Drops a partition from the given partitioned table.
   *
   * @param ms TODO
   * @param spec TODO
   * @throws HiveMetastoreException TODO
   */
  public static void dropPartition(HiveMetastoreClient ms, HiveObjectSpec spec)
    throws HiveMetastoreException {
    if (spec.isPartition()) {
      ms.dropPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName(), true);
      return;
    } else {
      throw new HiveMetastoreException("unpartitioned table provided" + spec.toString());
    }
  }
}
