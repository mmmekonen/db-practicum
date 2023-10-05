package common;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.File;

/*
 * Class to write tuples to a binary file, pages at a time
 */

public class TupleWriter {
  private static int PAGE_SIZE = 4096;
  private FileOutputStream fileOutputStream;
  private FileChannel fileChannel;
  private ByteBuffer buffer;
  private int tuplesWrittenOnPage;
  private int index;

  /*
   * Creates a new tuple writer for the given file
   * 
   * @param fileName The name of the file to write to
   * 
   * @throws IOException
   */
  public TupleWriter(String fileName) throws IOException {
    File file = new File(fileName);
    fileOutputStream = new FileOutputStream(file);
    fileChannel = fileOutputStream.getChannel();
    this.buffer = ByteBuffer.allocate(PAGE_SIZE); // Assuming each page has at most 4096 bytes
    this.tuplesWrittenOnPage = 0;
    this.index = 0;
  }

  /*
   * Creates a new tuple writer for the given file
   * 
   * @param file The file to write to
   * 
   * @throws IOException
   */
  public TupleWriter(File file) throws IOException {
    fileOutputStream = new FileOutputStream(file);
    fileChannel = fileOutputStream.getChannel();
    this.buffer = ByteBuffer.allocate(PAGE_SIZE); // Assuming each page has at most 4096 bytes
    this.tuplesWrittenOnPage = 0;
    this.index = 0;
  }

  /*
   * Writes the given tuple to the buffer
   * Checks if a new page needs to be created and/or written to disk
   * 
   * @param tuple The tuple to be written
   * 
   * @throws IOException
   */
  public void writeTuple(Tuple tuple) throws IOException {
    int tupleSize = tuple.getAllElements().size();

    if (tuplesWrittenOnPage == 0) {
      newPage(tupleSize);
    } else if ((index + (tupleSize * 4)) > PAGE_SIZE) {

      writePage();
      newPage(tupleSize);

    }

    for (int i = 0; i < tupleSize; i++) {
      buffer.putInt(tuple.getElementAtIndex(i));
      index += 4;
    }

    this.tuplesWrittenOnPage++;
    buffer.putInt(4, this.tuplesWrittenOnPage);
  }

  /*
   * Funtion to create a new page in the buffer and resets all required variables
   * 
   * @param tupleSize The size of the tuple to be written
   */
  private void newPage(int tupleSize) {
    buffer.clear();
    buffer.put(new byte[PAGE_SIZE]);
    buffer.clear();

    buffer.putInt(tupleSize);
    buffer.putInt(0);

    tuplesWrittenOnPage = 0;
    index = 8;
  }

  /*
   * Writes the current page to disk
   * 
   * @throws IOException
   */
  private void writePage() throws IOException {
    while (buffer.hasRemaining()) {
      buffer.putInt(0);
    }

    buffer.flip();
    fileChannel.write(buffer);
    buffer.clear();
  }

  /*
   * Closes the file and the file channel and writes the last page to disk
   * 
   * @throws IOException
   */
  public void close() throws IOException {
    if (tuplesWrittenOnPage != 0)
      writePage();

    fileChannel.close();
    fileOutputStream.close();
  }

}