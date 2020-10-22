package com.airbnb.hive;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;

import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.db.EmbeddedMySqlDb;
import com.airbnb.reair.db.StaticDbConnectionFactory;
import com.airbnb.reair.db.TestDbCredentials;
import com.airbnb.reair.hive.hooks.AuditCoreLogModule;
import com.airbnb.reair.hive.hooks.AuditLogHookUtils;
import com.airbnb.reair.hive.hooks.HiveOperation;
import com.airbnb.reair.hive.hooks.MetastoreAuditLogListener;
import com.airbnb.reair.hive.hooks.ObjectLogModule;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MetastoreAuditLogListenerTest {

  private static final Log LOG = LogFactory.getLog(
      MetastoreAuditLogListenerTest.class
  );

  protected static EmbeddedMySqlDb embeddedMySqlDb;

  protected static final String DB_NAME = "audit_log_db";
  protected static final String AUDIT_LOG_TABLE_NAME = "audit_log";
  protected static final String INPUT_OBJECTS_TABLE_NAME = "audit_objects";
  protected static final String OUTPUT_OBJECTS_TABLE_NAME = "audit_objects";
  protected static final String MAP_RED_STATS_TABLE_NAME = "mapred_stats";

  @BeforeClass
  public static void setupClass() {
    embeddedMySqlDb = new EmbeddedMySqlDb();
    embeddedMySqlDb.startDb();
  }

  /**
   * Generates a database connection factory for use in testing.
   *
   * @return The database connection factory
   * @throws SQLException If there's an error insert into the DB
   */
  public static DbConnectionFactory getDbConnectionFactory()
    throws SQLException {
    TestDbCredentials testDbCredentials = new TestDbCredentials();
    return new StaticDbConnectionFactory(
      ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb),
      testDbCredentials.getReadWriteUsername(),
      testDbCredentials.getReadWritePassword()
    );
  }

  /**
   * Resets the testing database.
   *
   * @throws SQLException If there's an error inserting to the DB
   */
  public static void resetState() throws SQLException {
    DbConnectionFactory dbConnectionFactory = getDbConnectionFactory();
    ReplicationTestUtils.dropDatabase(dbConnectionFactory, DB_NAME);
    AuditLogHookUtils.setupAuditLogTables(
        dbConnectionFactory,
        DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        MAP_RED_STATS_TABLE_NAME
    );
  }

  /**
   * Generates a Hive configuration using the test database credentials.
   *
   * @return The Hive configuration
   */
  public HiveConf getHiveConfig() {
    final TestDbCredentials testDbCredentials = new TestDbCredentials();

    HiveConf hiveConf = new HiveConf();

    hiveConf.set(
        MetastoreAuditLogListener.JDBC_URL_KEY,
        ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb, DB_NAME)
    );

    hiveConf.set(AuditCoreLogModule.TABLE_NAME_KEY, AUDIT_LOG_TABLE_NAME);
    hiveConf.set(ObjectLogModule.TABLE_NAME_KEY, OUTPUT_OBJECTS_TABLE_NAME);

    hiveConf.set(
        MetastoreAuditLogListener.DB_USERNAME,
        testDbCredentials.getReadWriteUsername()
    );

    hiveConf.set(
        MetastoreAuditLogListener.DB_PASSWORD,
        testDbCredentials.getReadWritePassword()
    );

    return hiveConf;
  }

  @Test
  public void testCreateDatabase() throws Exception {

    // Setup the audit log DB.
    resetState();

    final DbConnectionFactory dbConnectionFactory = getDbConnectionFactory();
    final HiveConf hiveConf = getHiveConfig();

    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    // Set up the source.
    Map<String, String> parameters = new HashMap<>();
    parameters.put("owner", "root");

    Database database = new Database(
        "test_db",
        "test database",
        "hdfs://dummy",
        parameters
    );

    CreateDatabaseEvent event = new CreateDatabaseEvent(database, true, null);
    metastoreAuditLogListener.onCreateDatabase(event);

    // Check the query audit log.
    List<String> auditCoreLogColumnsToCheck = Lists.newArrayList(
        "command_type",
        "command",
        "inputs",
        "outputs"
    );

    List<String> auditCoreLogRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        auditCoreLogColumnsToCheck,
        null
    );

    List<String> expectedDbRow = Lists.newArrayList(
        HiveOperation.THRIFT_CREATE_DATABASE.name(),
        "THRIFT_API",
        "{}",
        "{\"databases\":[\"test_db\"]}"
    );

    assertEquals(expectedDbRow, auditCoreLogRow);

    // Check the input objects audit log.
    List<String> inputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> inputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        INPUT_OBJECTS_TABLE_NAME,
        inputObjectsColumnsToCheck,
        "category = 'INPUT'"
    );

    assertEquals(null, inputObjectsRow);

    // Check the output objects audit log.
    List<String> outputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> outputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        outputObjectsColumnsToCheck,
        "category = 'OUTPUT'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db",
        "DATABASE",
        "{\"1\":{\"str\":\"test_db\"},\"2\":{\"str\":\"test database\"},\"3\":{"
          + "\"str\":\"hdfs://dummy\"},\"4\":{\"map\":[\"str\",\"str\",1,{\"own"
          + "er\":\"root\"}]}}"
    );

    assertEquals(expectedDbRow, outputObjectsRow);
  }

  @Test
  public void testDropDatabase() throws Exception {

    // Setup the audit log DB.
    resetState();

    final DbConnectionFactory dbConnectionFactory = getDbConnectionFactory();
    final HiveConf hiveConf = getHiveConfig();

    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    // Set up the source.
    Map<String, String> parameters = new HashMap<>();
    parameters.put("owner", "root");

    Database database = new Database(
        "test_db",
        "test database",
        "hdfs://dummy",
        parameters
    );

    DropDatabaseEvent event = new DropDatabaseEvent(database, true, null);
    metastoreAuditLogListener.onDropDatabase(event);

    // Check the query audit log.
    List<String> auditCoreLogColumnsToCheck = Lists.newArrayList(
        "command_type",
        "command",
        "inputs",
        "outputs"
    );

    List<String> auditCoreLogRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        auditCoreLogColumnsToCheck,
        null
    );

    List<String> expectedDbRow = Lists.newArrayList(
        HiveOperation.THRIFT_DROP_DATABASE.name(),
        "THRIFT_API",
        "{\"databases\":[\"test_db\"]}",
        "{}"
    );

    assertEquals(expectedDbRow, auditCoreLogRow);

    // Check the input objects audit log.
    List<String> inputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> inputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        INPUT_OBJECTS_TABLE_NAME,
        inputObjectsColumnsToCheck,
        "category = 'INPUT'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db",
        "DATABASE",
        "{\"1\":{\"str\":\"test_db\"},\"2\":{\"str\":\"test database\"},\"3\":{"
          + "\"str\":\"hdfs://dummy\"},\"4\":{\"map\":[\"str\",\"str\",1,{\"own"
          + "er\":\"root\"}]}}"
    );

    assertEquals(expectedDbRow, inputObjectsRow);

    // Check the output objects audit log.
    List<String> outputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> outputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        outputObjectsColumnsToCheck,
        "category = 'OUTPUT'"
    );

    assertEquals(null, outputObjectsRow);
  }

  @Test
  public void testCreateTable() throws Exception {

    // Setup the audit log DB.
    resetState();

    final DbConnectionFactory dbConnectionFactory = getDbConnectionFactory();
    final HiveConf hiveConf = getHiveConfig();

    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    // Set up the source.
    Table table = new Table();
    table.setDbName("test_db");
    table.setTableName("test_table");
    table.setOwner("root");

    Map<String, String> parameters = new HashMap<>();
    parameters.put("contact", "root@airbnb.com");
    table.setParameters(parameters);
    table.setSd(new StorageDescriptor());

    CreateTableEvent event = new CreateTableEvent(table, true, null);
    metastoreAuditLogListener.onCreateTable(event);

    // Check the query audit log.
    List<String> auditCoreLogColumnsToCheck = Lists.newArrayList(
        "command_type",
        "command",
        "inputs",
        "outputs"
    );

    List<String> auditCoreLogRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        auditCoreLogColumnsToCheck,
        null
    );

    List<String> expectedDbRow = Lists.newArrayList(
        HiveOperation.THRIFT_CREATE_TABLE.name(),
        "THRIFT_API",
        "{}",
        "{\"tables\":[\"test_db.test_table\"]}"
    );

    assertEquals(expectedDbRow, auditCoreLogRow);

    // Check the input objects audit log.
    List<String> inputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> inputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        INPUT_OBJECTS_TABLE_NAME,
        inputObjectsColumnsToCheck,
        "category = 'INPUT'"
    );

    assertEquals(null, inputObjectsRow);

    // Check the output objects audit log.
    List<String> outputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> outputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        outputObjectsColumnsToCheck,
        "category = 'OUTPUT'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db.test_table",
        "TABLE",
        "{\"1\":{\"str\":\"test_table\"},\"2\":{\"str\":\"test_db\"},\"3\":{\"s"
          + "tr\":\"root\"},\"4\":{\"i32\":0},\"5\":{\"i32\":0},\"6\":{\"i32\":"
          + "0},\"7\":{\"rec\":{\"5\":{\"tf\":0},\"6\":{\"i32\":0}}},\"9\":{\"m"
          + "ap\":[\"str\",\"str\",1,{\"contact\":\"root@airbnb.com\"}]}}"
    );

    assertEquals(expectedDbRow, outputObjectsRow);
  }

  @Test
  public void testDropTable() throws Exception {

    // Setup the audit log DB.
    resetState();

    final DbConnectionFactory dbConnectionFactory = getDbConnectionFactory();
    final HiveConf hiveConf = getHiveConfig();

    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    // Set up the source.
    Table table = new Table();
    table.setDbName("test_db");
    table.setTableName("test_table");
    table.setOwner("root");

    Map<String, String> parameters = new HashMap<>();
    parameters.put("contact", "root@airbnb.com");
    table.setParameters(parameters);
    table.setSd(new StorageDescriptor());

    DropTableEvent event = new DropTableEvent(table, true, true, null);
    metastoreAuditLogListener.onDropTable(event);

    // Check the query audit log.
    List<String> auditCoreLogColumnsToCheck = Lists.newArrayList(
        "command_type",
        "command",
        "inputs",
        "outputs"
    );

    List<String> auditCoreLogRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        auditCoreLogColumnsToCheck,
        null
    );

    List<String> expectedDbRow = Lists.newArrayList(
        HiveOperation.THRIFT_DROP_TABLE.name(),
        "THRIFT_API",
        "{\"tables\":[\"test_db.test_table\"]}",
        "{}"
    );

    assertEquals(expectedDbRow, auditCoreLogRow);

    // Check the input objects audit log.
    List<String> inputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> inputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        INPUT_OBJECTS_TABLE_NAME,
        inputObjectsColumnsToCheck,
        "category = 'INPUT'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db.test_table",
        "TABLE",
        "{\"1\":{\"str\":\"test_table\"},\"2\":{\"str\":\"test_db\"},\"3\":{\"s"
          + "tr\":\"root\"},\"4\":{\"i32\":0},\"5\":{\"i32\":0},\"6\":{\"i32\":"
          + "0},\"7\":{\"rec\":{\"5\":{\"tf\":0},\"6\":{\"i32\":0}}},\"9\":{\"m"
          + "ap\":[\"str\",\"str\",1,{\"contact\":\"root@airbnb.com\"}]}}"
    );

    assertEquals(expectedDbRow, inputObjectsRow);

    // Check the output objects audit log.
    List<String> outputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> outputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        outputObjectsColumnsToCheck,
        "category = 'OUTPUT'"
    );

    assertEquals(null, outputObjectsRow);
  }

  @Test
  public void testAlterTable() throws Exception {

    // Setup the audit log DB.
    resetState();

    final DbConnectionFactory dbConnectionFactory = getDbConnectionFactory();
    final HiveConf hiveConf = getHiveConfig();

    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    // Set up the source.
    Table oldTable = new Table();
    oldTable.setDbName("test_db");
    oldTable.setTableName("test_old_table");
    oldTable.setOwner("foo");
    oldTable.setSd(new StorageDescriptor());

    Map<String, String> oldParameters = new HashMap<>();
    oldParameters.put("contact", "foo@airbnb.com");
    oldTable.setParameters(oldParameters);

    Table newTable = new Table();
    newTable.setDbName("test_db");
    newTable.setTableName("test_new_table");
    newTable.setOwner("bar");
    newTable.setSd(new StorageDescriptor());

    Map<String, String> newParameters = new HashMap<>();
    newParameters.put("contact", "bar@airbnb.com");
    newTable.setParameters(newParameters);

    AlterTableEvent event = new AlterTableEvent(
        oldTable,
        newTable,
        true,
        null
    );

    metastoreAuditLogListener.onAlterTable(event);

    // Check the query audit log.
    List<String> auditCoreLogColumnsToCheck = Lists.newArrayList(
        "command_type",
        "command",
        "inputs",
        "outputs"
    );

    List<String> auditCoreLogRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        auditCoreLogColumnsToCheck,
        null
    );

    List<String> expectedDbRow = Lists.newArrayList(
        HiveOperation.THRIFT_ALTER_TABLE.name(),
        "THRIFT_API",
        "{\"tables\":[\"test_db.test_old_table\"]}",
        "{\"tables\":[\"test_db.test_new_table\"]}"
    );

    assertEquals(expectedDbRow, auditCoreLogRow);

    // Check the input objects audit log.
    List<String> inputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> inputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        INPUT_OBJECTS_TABLE_NAME,
        inputObjectsColumnsToCheck,
        "category = 'INPUT'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db.test_old_table",
        "TABLE",
        "{\"1\":{\"str\":\"test_old_table\"},\"2\":{\"str\":\"test_db\"},\"3\":"
          + "{\"str\":\"foo\"},\"4\":{\"i32\":0},\"5\":{\"i32\":0},\"6\":{\"i32"
          + "\":0},\"7\":{\"rec\":{\"5\":{\"tf\":0},\"6\":{\"i32\":0}}},\"9\":{"
          + "\"map\":[\"str\",\"str\",1,{\"contact\":\"foo@airbnb.com\"}]}}"
    );

    assertEquals(expectedDbRow, inputObjectsRow);

    // Check the output objects audit log.
    List<String> outputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> outputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        outputObjectsColumnsToCheck,
        "category = 'OUTPUT'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db.test_new_table",
        "TABLE",
        "{\"1\":{\"str\":\"test_new_table\"},\"2\":{\"str\":\"test_db\"},\"3\":"
          + "{\"str\":\"bar\"},\"4\":{\"i32\":0},\"5\":{\"i32\":0},\"6\":{\"i32"
          + "\":0},\"7\":{\"rec\":{\"5\":{\"tf\":0},\"6\":{\"i32\":0}}},\"9\":{"
          + "\"map\":[\"str\",\"str\",1,{\"contact\":\"bar@airbnb.com\"}]}}"
    );

    assertEquals(expectedDbRow, outputObjectsRow);
  }

  @Test
  public void testAddPartition() throws Exception {

    // Setup the audit log DB.
    resetState();

    final DbConnectionFactory dbConnectionFactory = getDbConnectionFactory();
    final HiveConf hiveConf = getHiveConfig();

    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    // Set up the source.
    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo());
    sd.setLocation("hdfs://dummy/");

    Table table = new Table();
    table.setDbName("test_db");
    table.setTableName("test_table");
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(sd);

    List<FieldSchema> partitionCols = new ArrayList<>();
    partitionCols.add(new FieldSchema("ds", "string", "UTC date"));
    table.setPartitionKeys(partitionCols);

    Partition partition = new Partition();
    partition.setDbName("test_db");
    partition.setTableName("test_table");
    partition.setSd(new StorageDescriptor());

    List<String> partitionValues = new ArrayList<>();
    partitionValues.add("2016-01-01");
    partition.setValues(partitionValues);

    AddPartitionEvent event = new AddPartitionEvent(
        table,
        partition,
        false,
        null
    );

    metastoreAuditLogListener.onAddPartition(event);

    // Check the query audit log.
    List<String> auditCoreLogColumnsToCheck = Lists.newArrayList(
        "command_type",
        "command",
        "inputs",
        "outputs"
    );

    List<String> auditCoreLogRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        auditCoreLogColumnsToCheck,
        null
    );

    List<String> expectedDbRow = Lists.newArrayList(
        HiveOperation.THRIFT_ADD_PARTITION.name(),
        "THRIFT_API",
        "{}",
        "{\"partitions\":[\"test_db.test_table/ds=2016-01-01\"]}"
    );

    assertEquals(expectedDbRow, auditCoreLogRow);

    // Check the input objects audit log.
    List<String> inputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> inputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        INPUT_OBJECTS_TABLE_NAME,
        inputObjectsColumnsToCheck,
        "category = 'INPUT'"
    );

    assertEquals(null, inputObjectsRow);

    // Check the output objects audit log.
    List<String> outputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> outputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        outputObjectsColumnsToCheck,
        "category = 'REFERENCE_TABLE' AND type = 'TABLE'"
    );

    expectedDbRow = Lists.newArrayList(
      "test_db.test_table",
      "TABLE",
      "{\"1\":{\"str\":\"test_table\"},\"2\":{\"str\":\"test_db\"},\"4\":{\"i32"
        + "\":0},\"5\":{\"i32\":0},\"6\":{\"i32\":0},\"7\":{\"rec\":{\"2\":{\"s"
        + "tr\":\"hdfs://dummy/\"},\"5\":{\"tf\":0},\"6\":{\"i32\":0},\"7\":{\""
        + "rec\":{}}}},\"8\":{\"lst\":[\"rec\",1,{\"1\":{\"str\":\"ds\"},\"2\":"
        + "{\"str\":\"string\"},\"3\":{\"str\":\"UTC date\"}}]},\"12\":{\"str\""
        + ":\"EXTERNAL_TABLE\"}}"
    );

    assertEquals(expectedDbRow, outputObjectsRow);

    outputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        outputObjectsColumnsToCheck,
        "category = 'OUTPUT' AND type = 'PARTITION'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db.test_table/ds=2016-01-01",
        "PARTITION",
        "{\"1\":{\"lst\":[\"str\",1,\"2016-01-01\"]},\"2\":{\"str\":\"test_db\""
          + "},\"3\":{\"str\":\"test_table\"},\"4\":{\"i32\":0},\"5\":{\"i32\":"
          + "0},\"6\":{\"rec\":{\"2\":{\"str\":\"hdfs://dummy/ds=2016-01-01\"},"
          + "\"5\":{\"tf\":0},\"6\":{\"i32\":0}}}}"
    );

    assertEquals(expectedDbRow, outputObjectsRow);
  }

  @Test
  public void testDropPartition() throws Exception {

    // Setup the audit log DB.
    resetState();

    final DbConnectionFactory dbConnectionFactory = getDbConnectionFactory();
    final HiveConf hiveConf = getHiveConfig();

    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    // Set up the source.
    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo());
    sd.setLocation("hdfs://dummy/");

    Table table = new Table();
    table.setDbName("test_db");
    table.setTableName("test_table");
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(sd);

    List<FieldSchema> partitionCols = new ArrayList<>();
    partitionCols.add(new FieldSchema("ds", "string", "UTC date"));
    table.setPartitionKeys(partitionCols);

    Partition partition = new Partition();
    partition.setDbName("test_db");
    partition.setTableName("test_table");
    partition.setSd(new StorageDescriptor());

    List<String> partitionValues = new ArrayList<>();
    partitionValues.add("2016-01-01");
    partition.setValues(partitionValues);

    DropPartitionEvent event = new DropPartitionEvent(
        table,
        partition,
        false,
        true,
        null
    );

    metastoreAuditLogListener.onDropPartition(event);

    // Check the query audit log.
    List<String> auditCoreLogColumnsToCheck = Lists.newArrayList(
        "command_type",
        "command",
        "inputs",
        "outputs"
    );

    List<String> auditCoreLogRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        auditCoreLogColumnsToCheck,
        null
    );

    List<String> expectedDbRow = Lists.newArrayList(
        HiveOperation.THRIFT_DROP_PARTITION.name(),
        "THRIFT_API",
        "{\"partitions\":[\"test_db.test_table/ds=2016-01-01\"]}",
        "{}"
    );

    assertEquals(expectedDbRow, auditCoreLogRow);

    // Check the input objects audit log.
    List<String> inputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> inputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        INPUT_OBJECTS_TABLE_NAME,
        inputObjectsColumnsToCheck,
        "category = 'REFERENCE_TABLE' AND type = 'TABLE'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db.test_table",
        "TABLE",
        "{\"1\":{\"str\":\"test_table\"},\"2\":{\"str\":\"test_db\"},\"4\":{\"i"
          + "32\":0},\"5\":{\"i32\":0},\"6\":{\"i32\":0},\"7\":{\"rec\":{\"2\":"
          + "{\"str\":\"hdfs://dummy/\"},\"5\":{\"tf\":0},\"6\":{\"i32\":0},\"7"
          + "\":{\"rec\":{}}}},\"8\":{\"lst\":[\"rec\",1,{\"1\":{\"str\":\"ds\""
          + "},\"2\":{\"str\":\"string\"},\"3\":{\"str\":\"UTC date\"}}]},\"12"
          + "\":{\"str\":\"EXTERNAL_TABLE\"}}"
    );

    assertEquals(expectedDbRow, inputObjectsRow);

    inputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        INPUT_OBJECTS_TABLE_NAME,
        inputObjectsColumnsToCheck,
        "category = 'INPUT' AND type = 'PARTITION'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db.test_table/ds=2016-01-01",
        "PARTITION",
        "{\"1\":{\"lst\":[\"str\",1,\"2016-01-01\"]},\"2\":{\"str\":\"test_db\""
          + "},\"3\":{\"str\":\"test_table\"},\"4\":{\"i32\":0},\"5\":{\"i32\":"
          + "0},\"6\":{\"rec\":{\"2\":{\"str\":\"hdfs://dummy/ds=2016-01-01\"},"
          + "\"5\":{\"tf\":0},\"6\":{\"i32\":0}}}}"
    );

    assertEquals(expectedDbRow, inputObjectsRow);

    // Check the output objects audit log.
    List<String> outputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> outputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        outputObjectsColumnsToCheck,
        "category = 'OUTPUT'"
    );

    assertEquals(null, outputObjectsRow);
  }

  @Test
  public void testAlterPartition() throws Exception {

    // Setup the audit log DB.
    resetState();

    final DbConnectionFactory dbConnectionFactory = getDbConnectionFactory();
    final HiveConf hiveConf = getHiveConfig();

    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    // Set up the source.
    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo());
    sd.setLocation("hdfs://dummy");

    Table table = new Table();
    table.setDbName("test_db");
    table.setTableName("test_table");
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(sd);

    List<FieldSchema> partitionCols = new ArrayList<>();
    partitionCols.add(new FieldSchema("ds", "string", "UTC date"));
    table.setPartitionKeys(partitionCols);

    Partition oldPartition = new Partition();
    oldPartition.setDbName("test_db");
    oldPartition.setTableName("test_table");
    oldPartition.setSd(sd);

    List<String> oldPartitionValues = new ArrayList<>();
    oldPartitionValues.add("2016-01-01");
    oldPartition.setValues(oldPartitionValues);

    Partition newPartition = new Partition();
    newPartition.setDbName("test_db");
    newPartition.setTableName("test_table");
    newPartition.setSd(sd);

    List<String> newPartitionValues = new ArrayList<>();
    newPartitionValues.add("2016-01-02");
    newPartition.setValues(newPartitionValues);

    HMSHandler handler = Mockito.mock(HMSHandler.class);

    Mockito.when(
        handler.get_table(
            "test_db",
            "test_table"
        )
    ).thenReturn(table);

    AlterPartitionEvent event = new AlterPartitionEvent(
        oldPartition,
        newPartition,
        false,
        handler
    );

    metastoreAuditLogListener.onAlterPartition(event);

    // Check the query audit log.
    List<String> auditCoreLogColumnsToCheck = Lists.newArrayList(
        "command_type",
        "command",
        "inputs",
        "outputs"
    );

    List<String> auditCoreLogRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        auditCoreLogColumnsToCheck,
        null
    );

    List<String> expectedDbRow = Lists.newArrayList(
        HiveOperation.THRIFT_ALTER_PARTITION.name(),
        "THRIFT_API",
        "{\"partitions\":[\"test_db.test_table/ds=2016-01-01\"]}",
        "{\"partitions\":[\"test_db.test_table/ds=2016-01-02\"]}"
    );

    assertEquals(expectedDbRow, auditCoreLogRow);

    // Check the input objects audit log. Note there is no easy way to verify
    // the table for an AlterPartitionEvent since we instantiate this within the
    // event listener where various attributes may be dynamic.
    List<String> inputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> inputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        INPUT_OBJECTS_TABLE_NAME,
        inputObjectsColumnsToCheck,
        "category = 'INPUT' AND type = 'PARTITION'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db.test_table/ds=2016-01-01",
        "PARTITION",
        "{\"1\":{\"lst\":[\"str\",1,\"2016-01-01\"]},\"2\":{\"str\":\"test_db\""
          + "},\"3\":{\"str\":\"test_table\"},\"4\":{\"i32\":0},\"5\":{\"i32\":"
          + "0},\"6\":{\"rec\":{\"2\":{\"str\":\"hdfs://dummy\"},\"5\":{\"tf\":"
          + "0},\"6\":{\"i32\":0},\"7\":{\"rec\":{}}}}}"
    );

    assertEquals(expectedDbRow, inputObjectsRow);

    // Check the output objects audit log. Note there is no easy way to verify
    // the table for an AlterPartitionEvent since we instantiate this within the
    // event listener where various attributes may be dynamic.
    List<String> outputObjectsColumnsToCheck = Lists.newArrayList(
        "name",
        "type",
        "serialized_object"
    );

    List<String> outputObjectsRow = ReplicationTestUtils.getRow(
        dbConnectionFactory,
        DB_NAME,
        OUTPUT_OBJECTS_TABLE_NAME,
        outputObjectsColumnsToCheck,
        "category = 'OUTPUT' AND type = 'PARTITION'"
    );

    expectedDbRow = Lists.newArrayList(
        "test_db.test_table/ds=2016-01-02",
        "PARTITION",
        "{\"1\":{\"lst\":[\"str\",1,\"2016-01-02\"]},\"2\":{\"str\":\"test_db\""
          + "},\"3\":{\"str\":\"test_table\"},\"4\":{\"i32\":0},\"5\":{\"i32\":"
          + "0},\"6\":{\"rec\":{\"2\":{\"str\":\"hdfs://dummy\"},\"5\":{\"tf\":"
          + "0},\"6\":{\"i32\":0},\"7\":{\"rec\":{}}}}}"
    );

    assertEquals(expectedDbRow, outputObjectsRow);
  }
}
