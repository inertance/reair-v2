package com.airbnb.reair.batch.hive;

import com.google.common.base.Joiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Split containing a Hive table name.
 */
public class HiveTablesInputSplit extends InputSplit implements Writable {
  private List<String> tables;

  public HiveTablesInputSplit(List<String> tables) {
    this.tables = tables;
  }

  public HiveTablesInputSplit() {}

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String toString() {
    return Joiner.on(",").join(tables);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(tables.size());
    for (String t : tables) {
      Text.writeString(dataOutput, t);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = dataInput.readInt();
    this.tables = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      this.tables.add(Text.readString(dataInput));
    }
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  public List<String> getTables() {
    return tables;
  }
}
