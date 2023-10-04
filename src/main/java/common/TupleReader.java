package common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class TupleReader {
  private FileInputStream fileInputStream;
  private FileChannel fileChannel;
  private ByteBuffer buffer;
  private int numTuples;
  private int attributes;
  private static int PAGE_SIZE = 4096;
  private int tuplesRemaining;

  public TupleReader(String fileName) throws IOException {
    this.fileInputStream = new FileInputStream(fileName);
    this.fileChannel = fileInputStream.getChannel();
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    readPageHeader();
  }

  public TupleReader(File file) throws IOException {
    this.fileInputStream = new FileInputStream(file);
    this.fileChannel = fileInputStream.getChannel();
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    readPageHeader();
  }

  private boolean readPageHeader() throws IOException {
    buffer.clear();
    if (fileChannel.read(buffer) != -1) {
      buffer.flip();
      this.attributes = buffer.getInt();
      this.numTuples = buffer.getInt();
      this.tuplesRemaining = numTuples;
      return true;

    } else {
      return false;
    }
  }

  public int getAttributes() {
    return attributes;
  }

  public int getNumTuples() {
    return numTuples;
  }

  public Tuple readNextTuple() throws IOException {
    // Read the next page header if the current page is exhausted
    if (buffer.remaining() < (attributes * 4)) {
      boolean more = readPageHeader();
      if (more == false) {
        return null;
      }
    }

    if (tuplesRemaining == 0) {
      return null;
    }

    ArrayList<Integer> tuple = new ArrayList<Integer>();
    for (int i = 0; i < attributes; i++) {
      int attributeValue = buffer.getInt();
      tuple.add(attributeValue);
    }

    this.tuplesRemaining--;

    Tuple t = new Tuple(tuple);
    return t;
  }

  public void close() throws IOException {
    fileChannel.close();
    fileInputStream.close();
  }

}