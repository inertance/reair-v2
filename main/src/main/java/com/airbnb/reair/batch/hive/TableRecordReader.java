package com.airbnb.reair.batch.hive;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * Record reader that returns DB / table names.
 */
public class TableRecordReader extends RecordReader<Text, Text> {
  private List<String> tables;
  private int index = 0;
  private String[] cur;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    if (!(inputSplit instanceof HiveTablesInputSplit)) {
      throw new IOException("Invalid split class passed in.");
    }

    this.tables = ((HiveTablesInputSplit) inputSplit).getTables();
    this.index = 0;
    this.cur = null;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (index < tables.size()) {
      cur = tables.get(index++).split(":");
      return true;
    }

    return false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    if (cur == null) {
      return null;
    }

    return new Text(cur[0]);
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    if (cur == null) {
      return null;
    }
    return new Text(cur[1]);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return ((float) index) / tables.size();
  }

  @Override
  public void close() throws IOException {}
}
