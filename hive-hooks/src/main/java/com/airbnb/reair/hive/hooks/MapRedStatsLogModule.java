package com.airbnb.reair.hive.hooks;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Group;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * A log module for logging mapreduce stats for each stage of a Hive query.
 */
public class MapRedStatsLogModule extends BaseLogModule {
  public static final String TABLE_NAME_KEY =
      "airbnb.reair.audit_log.mapred_stats.table_name";

  private final long auditLogId;

  public MapRedStatsLogModule(final Connection connection,
                              final SessionStateLite sessionStateLite,
                              long auditLogId)
           throws ConfigurationException {
    super(connection, TABLE_NAME_KEY, sessionStateLite);
    this.auditLogId = auditLogId;
  }

  /**
   * Runs the log module, writing the relevant audit data to the DB.
   *
   * @throws SerializationException if there's an error serializing data.
   */
  public void run() throws SerializationException, SQLException {
    final String query = String.format("INSERT INTO %s ("
        + "audit_log_id, "
        + "stage, "
        + "mappers, "
        + "reducers, "
        + "cpu_time, "
        + "counters) "
        + "VALUES (?, ?, ?, ?, ?, ?)",
        tableName);

    // Insert a DB row for each Hive stage
    Map<String, MapRedStats> statsPerStage = sessionStateLite.getMapRedStats();
    for (String stage: statsPerStage.keySet()) {
      MapRedStats stats = statsPerStage.get(stage);
      PreparedStatement ps = connection.prepareStatement(query);
      int psIndex = 1;
      ps.setLong(psIndex++, auditLogId);
      ps.setString(psIndex++, stage);
      ps.setLong(psIndex++, stats.getNumMap());
      ps.setLong(psIndex++, stats.getNumReduce());
      ps.setLong(psIndex++, stats.getCpuMSec());
      ps.setString(psIndex, toJson(stats.getCounters()));
      ps.executeUpdate();
    }
  }

  /**
   * Converts Hadoop counters to a JSON representation.
   *
   * @param counters the Hadoop counters to convert
   * @return the JSON representation of the given counters
   *
   * @throws SerializationException if mapping the counters to JSON fails
   */
  @VisibleForTesting
  static String toJson(Counters counters) throws SerializationException {
    ArrayNode countersJsonNode = JsonNodeFactory.instance.arrayNode();

    ArrayNode groupsJsonNode = JsonNodeFactory.instance.arrayNode();
    for (Group group: counters) {
      for (Counters.Counter counter: group) {
        ObjectNode counterJsonNode = JsonNodeFactory.instance.objectNode();
        counterJsonNode.put("counterName", counter.getName())
                       .put("value", counter.getValue());
        countersJsonNode.add(counterJsonNode);
      }
      ObjectNode groupJsonNode = JsonNodeFactory.instance.objectNode();
      groupJsonNode.put("groupName", group.getDisplayName())
                   .put("counters", countersJsonNode);
      groupsJsonNode.add(groupJsonNode);
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(groupsJsonNode);
    } catch (JsonProcessingException e) {
      throw new SerializationException(e);
    }
  }
}
