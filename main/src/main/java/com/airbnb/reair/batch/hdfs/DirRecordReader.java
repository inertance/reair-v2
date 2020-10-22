package com.airbnb.reair.batch.hdfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * Record Reader that returns paths to directories.
 */
public class DirRecordReader extends RecordReader<Text, Boolean> {
  private List<InputSplit> inputSplits;
  private int index = 0;
  private DirInputSplit cur;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    if (!(inputSplit instanceof ListDirInputSplit)) {
      throw new IOException("Invalid split class passed in.");
    }

    this.inputSplits = ((ListDirInputSplit) inputSplit).getSplits();
    this.index = 0;
    this.cur = null;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (index < inputSplits.size()) {
      cur = (DirInputSplit) inputSplits.get(index++);
      return true;
    }

    return false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    if (cur == null) {
      return null;
    }

    return new Text(cur.getFilePath());
  }

  @Override
  public Boolean getCurrentValue() throws IOException, InterruptedException {
    if (cur == null) {
      return null;
    }
    return cur.isLeafLevel();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return ((float) index + 1) / inputSplits.size();
  }

  @Override
  public void close() throws IOException {}
}
