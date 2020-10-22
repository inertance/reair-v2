package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ExchangePartitionParser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class ExchangePartitionParserTest {
  private static final Log LOG = LogFactory.getLog(ExchangePartitionParserTest.class);

  @Test
  public void testParse1() {
    String query = "ALTER TABLE test_db.test_table_exchange_to EXCHANGE "
        + "PARTITION( ds='1', hr = '2') "
        + "WITH TABLE test_db.test_table_exchange_from";

    ExchangePartitionParser parser = new ExchangePartitionParser();
    boolean parsed = parser.parse(query);
    assertTrue(parsed);
    assertEquals(new HiveObjectSpec("test_db", "test_table_exchange_from"),
        parser.getExchangeFromSpec());
    assertEquals(new HiveObjectSpec("test_db", "test_table_exchange_to"),
        parser.getExchangeToSpec());
    assertEquals("ds=1/hr=2", parser.getPartitionName());
  }

  @Test
  public void testParse2() {
    String query = "ALTER TABLE test_table_exchange_to EXCHANGE " + "PARTITION(ds='1', hr='2') "
        + "WITH TABLE test_table_exchange_from";

    ExchangePartitionParser parser = new ExchangePartitionParser();
    boolean parsed = parser.parse(query);
    assertTrue(parsed);
    assertEquals(new HiveObjectSpec("default", "test_table_exchange_from"),
        parser.getExchangeFromSpec());
    assertEquals(new HiveObjectSpec("default", "test_table_exchange_to"),
        parser.getExchangeToSpec());
    assertEquals("ds=1/hr=2", parser.getPartitionName());
  }
}
