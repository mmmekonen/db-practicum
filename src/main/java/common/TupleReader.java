package common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Class to read tuples from a binary file, pages at a time
 */
public class TupleReader {
  private FileInputStream fileInputStream;
  private FileChannel fileChannel;
  private ByteBuffer buffer;
  private int numTuples;
  private int attributes;
  private static int PAGE_SIZE = 4096;
  private int tuplesRemaining;
  private int currentPID;
  private int currentTID;

  /**
   * Creates a new tuple reader for the given file
   *
   * @param fileName The name of the file to read from
   *
   * @throws IOException
   */
  public TupleReader(String fileName) throws IOException {
    this.fileInputStream = new FileInputStream(fileName);
    this.fileChannel = fileInputStream.getChannel();
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    this.currentPID = -1;
    this.currentTID = 2;
    readPageHeader();
  }

  /**
   * Creates a new tuple reader for the given file
   *
   * @param file The file to read from
   *
   * @throws IOException
   */
  public TupleReader(File file) throws IOException {
    this.fileInputStream = new FileInputStream(file);
    this.fileChannel = fileInputStream.getChannel();
    this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    this.currentPID = -1;
    this.currentTID = 2;
    readPageHeader();
  }

  /**
   * Reads the next page header from the file to get the attributes and number of
   * tuples
   */
  private boolean readPageHeader() throws IOException {
    buffer.clear();
    if (fileChannel.read(buffer) != -1) {
      buffer.flip();
      this.attributes = buffer.getInt();
      this.numTuples = buffer.getInt();
      this.tuplesRemaining = numTuples;
      currentPID++;
      currentTID = 2;
      return true;

    } else {
      return false;
    }
  }

  /**
   * Returns the number of attributes in the tuples
   */
  public int getAttributes() {
    return attributes;
  }

  /**
   * Returns the number of tuples in the file
   */
  public int getNumTuples() {
    return numTuples;
  }

  /**
   * Reads the next tuple from the buffer
   *
   * @throws IOException
   */
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

    t.setTID(currentTID);
    t.setPID(currentPID);
    currentTID++;

    return t;
  }

  /**
   * Closes the file and the file channel
   *
   * @throws IOException
   */
  public void close() throws IOException {
    fileChannel.close();
    fileInputStream.close();
  }
}
