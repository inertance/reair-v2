package com.airbnb.reair.common;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 * Command line utilities.
 */
public class CliUtils {
  public static void printHelp(String command, Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(command, options);
  }
}
