package com.airbnb.reair.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Thread that reads from a stream and writes what it read to log4j.
 */
public class StreamLogger extends Thread {

  private static final Log LOG = LogFactory.getLog(StreamLogger.class);

  private InputStream inputStream;
  private boolean saveToString;
  private String streamAsString;

  /**
   * TODO.
   *
   * @param inputStream TODO
   * @param saveToString whether to return the entire output from the input stream as a single
   *        string.
   */
  public StreamLogger(String threadName, InputStream inputStream, boolean saveToString) {
    this.inputStream = inputStream;
    this.saveToString = saveToString;
    this.streamAsString = null;
    setName(threadName);
    setDaemon(true);
  }

  @Override
  public void run() {
    try {
      StringBuilder sb = new StringBuilder();
      InputStreamReader isr = new InputStreamReader(inputStream);
      BufferedReader br = new BufferedReader(isr);
      String line;
      while ((line = br.readLine()) != null) {
        LOG.debug(line);
        if (saveToString) {
          sb.append(line);
        }
      }
      streamAsString = sb.toString();
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  public String getStreamAsString() {
    return streamAsString;
  }
}
