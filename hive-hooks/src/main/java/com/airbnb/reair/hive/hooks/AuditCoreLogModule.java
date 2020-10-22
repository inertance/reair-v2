package com.airbnb.reair.hive.hooks;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Log module for adding core query audit data for a query to the DB.
 */
public class AuditCoreLogModule extends BaseLogModule {

  public static Logger LOG = Logger.getLogger(AuditCoreLogModule.class);

  public static final String TABLE_NAME_KEY =
      "airbnb.reair.audit_log.core.table_name";

  private final Set<ReadEntity> readEntities;
  private final Set<WriteEntity> writeEntities;
  private final UserGroupInformation userGroupInformation;

  /**
   * Constructor.
   *
   * @param connection the connection to use for connecting to the DB
   * @param sessionStateLite session information from Hive for this query
   * @param readEntities the entities that were read by the query
   * @param writeEntities the entities that were written by the query
   * @param userGroupInformation information about the user that ran the query
   *
   * @throws ConfigurationException if there's an error with the configuration for the hook
   */
  public AuditCoreLogModule(final Connection connection,
                            final SessionStateLite sessionStateLite,
                            final Set<ReadEntity> readEntities,
                            final Set<WriteEntity> writeEntities,
                            final UserGroupInformation userGroupInformation)
           throws ConfigurationException {
    super(connection, TABLE_NAME_KEY, sessionStateLite);
    this.readEntities = readEntities;
    this.writeEntities = writeEntities;
    this.userGroupInformation = userGroupInformation;
  }

  /**
   * Inserts the core audit data into the DB.
   *
   * @return the id for the inserted core audit log entry
   *
   * @throws EntityException if there's an error processing the entities associated with this query
   * @throws SQLException if there's an error querying the DB
   * @throws UnknownHostException if there's an error getting the IP of this host
   */
  public long run()
      throws EntityException, SerializationException, SQLException, UnknownHostException {
    final String query = String.format("INSERT INTO %s ("
        + "query_id, "
        + "command_type, "
        + "command, "
        + "inputs, "
        + "outputs, "
        + "username, "
        + "ip) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?)",
        tableName);

    // Write the main audit log entry
    int psIndex = 1;
    PreparedStatement ps = connection.prepareStatement(query,
                               Statement.RETURN_GENERATED_KEYS);
    ps.setString(psIndex++, sessionStateLite.getQueryId());
    ps.setString(psIndex++, sessionStateLite.getCommandType());
    ps.setString(psIndex++, sessionStateLite.getCmd());
    ps.setString(psIndex++, toJson(readEntities, true));
    ps.setString(psIndex++, toJson(writeEntities, true));
    ps.setString(psIndex++, userGroupInformation == null ? null :
                            userGroupInformation.getUserName());
    ps.setString(psIndex++, InetAddress.getLocalHost().getHostAddress());
    ps.executeUpdate();

    ResultSet rs = ps.getGeneratedKeys();
    rs.next();
    return rs.getLong(1);
  }

  /**
   * Converts the entities into a JSON object. Resulting object will look
   * like:
   * {
   *   "tables": [t1, t2...],
   *   "partitions": [p1, p2...],
   *   "dummy_partitions": [p1, p2...],
   *   "local_directories": [d1, d2...],
   *   "dfs_directories": [d1, d2...]
   * }
   *
   * <p>Where t1... and p1... objects are JSON objects that represent the thrift
   * metadata object. If identifierOnly is true, then only a short string
   * representation of the object will be used instead. e.g.
   * "default.my_table" or "default.my_partitioned_table/ds=1"
   *
   * @param entities the entities to convert into JSON
   * @param identifierOnly whether to use identifiers instead of the full JSON representation
   * @return a JSON string representing the entities
   * @throws EntityException if there's an error processing an entity
   * @throws SerializationException if there's an error serializing to JSON
   */
  private static String toJson(Collection<? extends Entity> entities,
                               boolean identifierOnly)
      throws EntityException, SerializationException {

    if (entities == null) {
      return new JSONObject().toString();
    }

    List<Database> databases = new ArrayList<>();
    List<Table> tables = new ArrayList<>();
    List<Partition> partitions = new ArrayList<>();
    List<Partition> dummyPartitions = new ArrayList<>();
    List<String> localDirectories = new ArrayList<>();
    List<String> dfsDirectories = new ArrayList<>();

    Map<Partition, String> partitionNames =
        new HashMap<>();

    for (Entity e : entities) {
      switch (e.getType()) {
        case DATABASE:
          databases.add(e.getDatabase());
          break;
        case TABLE:
          tables.add(e.getTable().getTTable());
          break;
        case PARTITION:
          partitions.add(e.getPartition().getTPartition());
          partitionNames.put(e.getPartition().getTPartition(),
              e.getPartition().getName());
          break;
        case DUMMYPARTITION:
          dummyPartitions.add(e.getPartition().getTPartition());
          partitionNames.put(e.getPartition().getTPartition(),
              e.getPartition().getName());
          break;
        case LOCAL_DIR:
          try {
            localDirectories.add(e.getLocation().toString());
          } catch (Exception ex) {
            throw new EntityException(ex);
          }
          break;
        case DFS_DIR:
          try {
            dfsDirectories.add(e.getLocation().toString());
          } catch (Exception ex) {
            throw new EntityException(ex);
          }
          break;
        case UDF:
          LOG.info(
              "Skipping logging of UDF type to audit log - "
                  + "displayName: " + e.getUDF().getDisplayName());
          break;
        default:
          throw new EntityException("Unhandled type: "
              + e.getType() + " entity: " + e);
      }
    }

    TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());

    JSONArray jsonDatabases = new JSONArray();
    JSONArray jsonTables = new JSONArray();
    JSONArray jsonPartitions = new JSONArray();
    JSONArray jsonDummyPartitions = new JSONArray();
    JSONArray jsonLocalDirs = new JSONArray();
    JSONArray jsonDfsDirs = new JSONArray();
    JSONObject obj = new JSONObject();

    try {
      for (Database db : databases) {
        if (identifierOnly) {
          String jsonDatabase = String.format("%s", db.getName());
          jsonDatabases.put(jsonDatabase);
        } else {
          jsonDatabases.put(new JSONObject(serializer.toString(db)));
        }
      }

      for (Table t : tables) {
        if (identifierOnly) {
          String jsonTable = String.format("%s.%s", t.getDbName(),
              t.getTableName());
          jsonTables.put(jsonTable);
        } else {
          jsonTables.put(new JSONObject(serializer.toString(t)));
        }
      }

      for (Partition p : partitions) {
        if (identifierOnly) {
          String partitionName = String.format("%s.%s/%s", p.getDbName(),
              p.getTableName(),
              partitionNames.get(p));
          jsonPartitions.put(partitionName);
        } else {
          jsonPartitions.put(new JSONObject(serializer.toString(p)));
        }
      }

      for (Partition p : dummyPartitions) {
        if (identifierOnly) {
          String dummyPartitionJson = String.format("%s.%s/%s", p.getDbName(),
              p.getTableName(),
              partitionNames.get(p));
          jsonDummyPartitions.put(dummyPartitionJson);
        } else {
          jsonDummyPartitions.put(new JSONObject(serializer.toString(p)));
        }
      }

      for (String dir : localDirectories) {
        jsonLocalDirs.put(dir);
      }

      for (String dir : dfsDirectories) {
        jsonDfsDirs.put(dir);
      }

      if (jsonDatabases.length() > 0) {
        obj.put("databases", jsonDatabases);
      }

      if (jsonTables.length() > 0) {
        obj.put("tables", jsonTables);
      }

      if (jsonPartitions.length() > 0) {
        obj.put("partitions", jsonPartitions);
      }

      if (jsonDummyPartitions.length() > 0) {
        obj.put("dummy_partitions", jsonDummyPartitions);
      }

      if (jsonLocalDirs.length() > 0) {
        obj.put("local_directories", jsonLocalDirs);
      }

      if (jsonDfsDirs.length() > 0) {
        obj.put("dfs_directories", jsonDfsDirs);
      }
    } catch (TException | JSONException e) {
      throw new SerializationException(e);
    }

    return obj.toString();
  }
}
