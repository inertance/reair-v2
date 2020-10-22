package com.airbnb.reair.batch.hdfs;

import com.google.common.base.Joiner;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits containing paths to directories.
 */
public class ListDirInputSplit extends InputSplit implements Writable {
  private List<InputSplit> splits;

  public ListDirInputSplit(List<InputSplit> splits) {
    this.splits = splits;
  }

  public ListDirInputSplit() {}

  public List<InputSplit> getSplits() {
    return splits;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(splits.size());
    for (InputSplit s : splits) {
      ((DirInputSplit) s).write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = dataInput.readInt();
    this.splits = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      DirInputSplit split = new DirInputSplit();
      split.readFields(dataInput);
      this.splits.add(split);
    }
  }

  @Override
  public String toString() {
    return "[" + Joiner.on(",").join(splits) + "]";
  }
}
