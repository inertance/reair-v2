package com.airbnb.reair.hive.hooks;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.mapred.Counters;
import org.junit.Test;

public class MapRedStatsLogModuleTest {

  @Test
  public void testZeroCounterGroupsToJson() throws SerializationException {
    Counters counters = new Counters();
    String json = MapRedStatsLogModule.toJson(counters);
    assertEquals(json, "[]");
  }

  @Test
  public void testOneGroupOneCounterToJson() throws SerializationException {
    Counters counters = new Counters();
    counters.incrCounter("SomeCounterGroupName", "SomeCounterName", 3);
    String json = MapRedStatsLogModule.toJson(counters);
    assertEquals(
        "[{\"groupName\":\"SomeCounterGroupName\",\"counters\":[{\"counterNa"
          + "me\":\"SomeCounterName\",\"value\":3}]}]",
        json);
  }

  @Test
  public void testOneGroupManyCountersToJson() throws SerializationException {
    Counters counters = new Counters();
    counters.incrCounter("SomeCounterGroupName", "SomeCounterName", 3);
    counters.incrCounter("SomeCounterGroupName", "AnotherCounterName", 4);
    counters.incrCounter("SomeCounterGroupName", "YetAnotherCounterName", 4);
    String json = MapRedStatsLogModule.toJson(counters);
    assertEquals(
        "[{\"groupName\":\"SomeCounterGroupName\",\"counters\":[{\"counterNam"
          + "e\":\"AnotherCounterName\",\"value\":4},{\"counterName\":\"SomeCount"
          + "erName\",\"value\":3},{\"counterName\":\"YetAnotherCounterName\",\"v"
          + "alue\":4}]}]",
        json);
  }

  @Test
  public void testManyGroupsManyCountersToJson()
      throws SerializationException {
    Counters counters = new Counters();
    counters.incrCounter("SomeCounterGroupName1", "SomeCounterName1", 3);
    counters.incrCounter("SomeCounterGroupName1", "SomeCounterName2", 4);
    counters.incrCounter("SomeCounterGroupName1", "SomeCounterName3", 5);
    counters.incrCounter("SomeCounterGroupName2", "SomeCounterName1", 6);
    counters.incrCounter("SomeCounterGroupName2", "SomeCounterName2", 7);
    counters.incrCounter("SomeCounterGroupName2", "SomeCounterName3", 8);
    counters.incrCounter("SomeCounterGroupName3", "SomeCounterName1", 9);
    counters.incrCounter("SomeCounterGroupName3", "SomeCounterName2", 10);
    counters.incrCounter("SomeCounterGroupName3", "SomeCounterName3", 11);
    String json = MapRedStatsLogModule.toJson(counters);
    assertEquals(
        "[{\"groupName\":\"SomeCounterGroupName1\",\"counters\":[{\"counterN"
          + "ame\":\"SomeCounterName1\",\"value\":3},{\"counterName\":\"SomeCount"
          + "erName2\",\"value\":4},{\"counterName\":\"SomeCounterName3\",\"value"
          + "\":5},{\"counterName\":\"SomeCounterName1\",\"value\":6},{\"counterN"
          + "ame\":\"SomeCounterName2\",\"value\":7},{\"counterName\":\"SomeCount"
          + "erName3\",\"value\":8},{\"counterName\":\"SomeCounterName1\",\"value"
          + "\":9},{\"counterName\":\"SomeCounterName2\",\"value\":10},{\"counter"
          + "Name\":\"SomeCounterName3\",\"value\":11}]},{\"groupName\":\"SomeCou"
          + "nterGroupName2\",\"counters\":[{\"counterName\":\"SomeCounterName1\""
          + ",\"value\":3},{\"counterName\":\"SomeCounterName2\",\"value\":4},{\""
          + "counterName\":\"SomeCounterName3\",\"value\":5},{\"counterName\":\"S"
          + "omeCounterName1\",\"value\":6},{\"counterName\":\"SomeCounterName2\""
          + ",\"value\":7},{\"counterName\":\"SomeCounterName3\",\"value\":8},{\""
          + "counterName\":\"SomeCounterName1\",\"value\":9},{\"counterName\":\"S"
          + "omeCounterName2\",\"value\":10},{\"counterName\":\"SomeCounterName3" 
          + "\",\"value\":11}]},{\"groupName\":\"SomeCounterGroupName3\",\"counte"
          + "rs\":[{\"counterName\":\"SomeCounterName1\",\"value\":3},{\"counterN"
          + "ame\":\"SomeCounterName2\",\"value\":4},{\"counterName\":\"SomeCount"
          + "erName3\",\"value\":5},{\"counterName\":\"SomeCounterName1\",\"value"
          + "\":6},{\"counterName\":\"SomeCounterName2\",\"value\":7},{\"counterN"
          + "ame\":\"SomeCounterName3\",\"value\":8},{\"counterName\":\"SomeCount"
          + "erName1\",\"value\":9},{\"counterName\":\"SomeCounterName2\",\"value"
          + "\":10},{\"counterName\":\"SomeCounterName3\",\"value\":11}]}]",
        json);
  }
}
