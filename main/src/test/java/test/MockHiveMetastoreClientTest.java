package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;

import com.airbnb.reair.common.HiveMetastoreException;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for the fake metastore client that we'll use later for testing.
 */
public class MockHiveMetastoreClientTest {
  private static MockHiveMetastoreClient mockHiveMetastoreClient;

  @Before
  public void setUp() {
    mockHiveMetastoreClient = new MockHiveMetastoreClient();
  }

  @Test
  public void testCreateAndDropTable() throws HiveMetastoreException {
    final String dbName = "test_db";
    final String tableName = "test_table";

    // First create a db and a table
    mockHiveMetastoreClient.createDatabase(new Database(dbName, null, null, null));
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    mockHiveMetastoreClient.createTable(table);

    // Verify that you get the same table back
    assertEquals(mockHiveMetastoreClient.getTable(dbName, tableName), table);

    // Drop it
    mockHiveMetastoreClient.dropTable(dbName, tableName, false);

    // Verify that you can't get the table any more
    assertNull(mockHiveMetastoreClient.getTable(dbName, tableName));
    assertFalse(mockHiveMetastoreClient.existsTable(dbName, tableName));
  }

  @Test
  public void testCreateAndDropPartition() throws HiveMetastoreException {
    final String dbName = "test_db";
    final String tableName = "test_table";
    final String partitionName = "ds=1/hr=2";
    final List<String> partitionValues = new ArrayList<>();
    partitionValues.add("1");
    partitionValues.add("2");

    // First create the db and a partitioned table
    mockHiveMetastoreClient.createDatabase(new Database(dbName, null, null, null));
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);

    List<FieldSchema> partitionCols = new ArrayList<>();
    partitionCols.add(new FieldSchema("ds", "string", "my ds comment"));
    partitionCols.add(new FieldSchema("hr", "string", "my hr comment"));
    table.setPartitionKeys(partitionCols);

    mockHiveMetastoreClient.createTable(table);

    // Then try adding a partition
    Partition partition = new Partition();
    partition.setDbName(dbName);
    partition.setTableName(tableName);
    partition.setValues(partitionValues);
    mockHiveMetastoreClient.addPartition(partition);

    // Verify that you get back the same partition
    assertEquals(mockHiveMetastoreClient.getPartition(dbName, tableName, partitionName), partition);

    // Try dropping the partition and verify that it doesn't exist
    mockHiveMetastoreClient.dropPartition(dbName, tableName, partitionName, false);
    assertNull(mockHiveMetastoreClient.getPartition(dbName, tableName, partitionName));
    assertFalse(mockHiveMetastoreClient.existsPartition(dbName, tableName, partitionName));

    // Try adding a partition again
    mockHiveMetastoreClient.addPartition(partition);

    // Drop the table
    mockHiveMetastoreClient.dropTable(dbName, tableName, false);

    // Verify that the partition doesn't exist
    assertNull(mockHiveMetastoreClient.getPartition(dbName, tableName, partitionName));
  }

  @Test
  public void testPartitionNameToMap() throws HiveMetastoreException {
    String partitionName = "ds=1/hr=2/min=3/sec=4";
    LinkedHashMap<String, String> expectedKeyValueMap = new LinkedHashMap<>();
    expectedKeyValueMap.put("ds", "1");
    expectedKeyValueMap.put("hr", "2");
    expectedKeyValueMap.put("min", "3");
    expectedKeyValueMap.put("sec", "4");

    Map<String, String> keyValueMap = mockHiveMetastoreClient.partitionNameToMap(partitionName);

    // Double check if iteration over the keySet / values is defined
    assertEquals(expectedKeyValueMap.keySet(), keyValueMap.keySet());
    assertEquals(Lists.newArrayList(expectedKeyValueMap.values()),
        Lists.newArrayList(keyValueMap.values()));
  }

  @Test
  public void testRenameTable() throws HiveMetastoreException {
    final String dbName = "test_db";
    final String tableName = "test_table";
    final String newTableName = "new_test_table";

    // First create the DB and the table
    mockHiveMetastoreClient.createDatabase(new Database(dbName, null, null, null));
    final Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    mockHiveMetastoreClient.createTable(table);

    // Verify that you get the same table back
    assertEquals(mockHiveMetastoreClient.getTable(dbName, tableName), table);

    // Rename it
    final Table newTable = new Table(table);
    newTable.setTableName(newTableName);
    mockHiveMetastoreClient.alterTable(dbName, tableName, newTable);

    // Verify that you can't get the old table any more
    assertNull(mockHiveMetastoreClient.getTable(dbName, tableName));

    // Verify that you can get the new table
    assertEquals(mockHiveMetastoreClient.getTable(dbName, newTableName), newTable);
  }
}
