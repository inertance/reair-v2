package com.airbnb.reair;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class DateUtils {
  /**
   * Converts the unix time into an ISO8601 time.
   *
   * @param time unix time to convert
   * @return a String representing the specified unix time
   */
  public String convertToIso8601(long time) {
    TimeZone tz = TimeZone.getTimeZone("UTC");
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
    df.setTimeZone(tz);
    return df.format(time);
  }
}
