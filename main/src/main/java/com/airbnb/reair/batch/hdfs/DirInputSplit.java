package com.airbnb.reair.batch.hdfs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Splits containing a path to a directory.
 */
public class DirInputSplit extends InputSplit implements Writable {
  private String filePath;
  private boolean leafLevel;

  public String getFilePath() {
    return filePath;
  }

  public boolean isLeafLevel() {
    return leafLevel;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String toString() {
    return filePath + ":" + leafLevel;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Text.writeString(dataOutput, this.filePath);
    dataOutput.writeBoolean(leafLevel);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.filePath = Text.readString(dataInput);
    this.leafLevel = dataInput.readBoolean();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  public DirInputSplit() {}

  public DirInputSplit(String filePath, boolean leaf) {
    this.filePath = filePath;
    this.leafLevel = leaf;
  }
}
