package com.airbnb.reair.incremental;

import com.airbnb.reair.common.HiveObjectSpec;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses exchange partition query commands. Since it uses a regex, there are cases that won't be
 * handled properly. See warnings below for details.
 *
 * <p>TODO: Deprecate once HIVE-12865 is resolved
 */
public class ExchangePartitionParser {

  private static String EXCHANGE_REGEX =
      "\\s*ALTER\\s+TABLE\\s+" + "(?<exchangeToTable>\\S+)\\s+EXCHANGE\\s+PARTITION\\s*\\(\\s*"
          + "(?<partitionSpec>.*)\\)\\s+WITH\\s+TABLE\\s+" + "(?<exchangeFromTable>\\S+)\\s*";

  private HiveObjectSpec exchangeFromTableSpec;
  private HiveObjectSpec exchangeToTableSpec;
  private String partitionName;
  private List<String> partitionValues;

  /**
   * Parse the query and store relevant attributes.
   *
   * @param query the query to parse
   * @return whether query was successfully parsed
   */
  public boolean parse(String query) {
    System.out.println("parse exchage ...");
    Matcher matcher = Pattern.compile(EXCHANGE_REGEX, Pattern.CASE_INSENSITIVE).matcher(query);

    if (!matcher.matches()) {
      System.out.println("unmatch exchange_regex");
      return false;
    }

    System.out.println("match exchange_regex");

    String exchangeFromTable = matcher.group("exchangeFromTable");
    String exchangeToTable = matcher.group("exchangeToTable");
    String partitionSpec = matcher.group("partitionSpec");

    exchangeFromTableSpec = getSpec(exchangeFromTable);
    exchangeToTableSpec = getSpec(exchangeToTable);
    partitionName = getPartitionName(partitionSpec);
    partitionValues = getPartitionValues(partitionSpec);
    return true;
  }

  /**
   * Parse the string representation of a Hive object specification into HiveObjectSpec.
   *
   * @param spec table specification in the form "db.table"
   * @return HiveObjectSpec represented by the input string
   */
  private HiveObjectSpec getSpec(String spec) {
    String[] specSplit = spec.split("\\.");
    if (specSplit.length == 1) {
      // Warning! Assumes default DB.
      return new HiveObjectSpec("default", specSplit[0]);
    } else if (specSplit.length == 2) {
      return new HiveObjectSpec(specSplit[0], specSplit[1]);
    } else {
      throw new RuntimeException(
          "Unexpected split from " + spec + " to " + Arrays.asList(specSplit));
    }
  }

  private String trimWhitespace(String str) {
    str = StringUtils.stripEnd(str, " \t\n");
    str = StringUtils.stripStart(str, " \t\n");
    return str;
  }

  /**
   * Get the partition that is being exchanged by this query.
   *
   * @return the name of the partition that is exchanged by the previously parsed query
   */
  public String getPartitionName() {
    return partitionName;
  }

  /**
   * Get the partition names from a partition spec. E.g. "PARTITION(ds='1')" => "ds=1"
   *
   * @param partitionSpec a partition specification in the form "ds=1, hr=2"
   *
   * @return the partition spec converted to that name
   */
  private String getPartitionName(String partitionSpec) {
    // Warning - incorrect for cases where there are commas in values and
    // other corner cases.
    String[] partitionSpecSplit = partitionSpec.split(",");
    StringBuilder sb = new StringBuilder();

    for (String columnSpec : partitionSpecSplit) {
      columnSpec = trimWhitespace(columnSpec);
      // columnSpec should be something of the form ds='1'
      String[] columnSpecSplit = columnSpec.split("=");
      if (columnSpecSplit.length != 2) {
        throw new RuntimeException("Unexpected column spec " + columnSpec);
      }

      if (sb.length() != 0) {
        sb.append("/");
      }
      String partitionColumnName = trimWhitespace(columnSpecSplit[0]);
      String partitionColumnValue = trimWhitespace(columnSpecSplit[1]).replace("'", "");
      sb.append(partitionColumnName);
      sb.append("=");
      sb.append(partitionColumnValue);
    }

    return sb.toString();
  }

  /**
   * Get the partition that is being exchanged by this query.
   *
   * @return the partition values of the partition that is exchanged by the previously parsed query
   */
  public List<String> getPartitionValues() {
    return partitionValues;
  }

  private List<String> getPartitionValues(String partitionSpec) {
    // Warning - incorrect for cases where there are commas in values and
    // other corner cases.
    String[] partitionSpecSplit = partitionSpec.split(",");
    List<String> partitionValues = new ArrayList<>();

    for (String columnSpec : partitionSpecSplit) {
      columnSpec = StringUtils.stripEnd(columnSpec, " \t\n");
      columnSpec = StringUtils.stripStart(columnSpec, " \t\n");
      // columnSpec should be something of the form ds='1'
      String[] columnSpecSplit = columnSpec.split("=");
      if (columnSpecSplit.length != 2) {
        throw new RuntimeException("Unexpected column spec " + columnSpec);
      }
      String partitionColumnValue = columnSpecSplit[1].replace("'", "");
      partitionValues.add(partitionColumnValue);
    }
    return partitionValues;
  }

  /**
   * Get the specification for the object that the query exchanged from.
   *
   * @return specification for the object that the query exchanged from.
   */
  public HiveObjectSpec getExchangeFromSpec() {
    return exchangeFromTableSpec;
  }

  /**
   * Get the specification for the object that the query exchanged to.
   *
   * @return specification for the object that the query exchanged to.
   */
  public HiveObjectSpec getExchangeToSpec() {
    return exchangeToTableSpec;
  }
}
