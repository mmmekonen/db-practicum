package physical_operator;

import common.DBCatalog;
import common.SortComparator;
import common.Tuple;
import common.TupleReader;
import common.TupleWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * A class to represent an external sort operator on a relation. Sorts each tuple in the order of
 * columns in orderbyElements and then in the order of the remaining columns in it's child
 * operator's outputSchema. Only uses the specified amount of buffer space to do the sorting and
 * merging.
 */
public class ExternalSortOperator extends Operator {

  private Operator child;
  private ArrayList<Tuple> buffer;
  private int bufferSize;
  private Tuple currTuple;
  private File directory;
  private File sortedFile;
  private TupleReader readerSortedFile;
  private final ArrayList<Column> orderByElements;
  private HashMap<Integer, Integer> indexOrders;

  /**
   * Creates an external sort operator using an Operator, a List of Columns to order by, and a
   * specified buffer size.
   *
   * @param child The sort operator's child operator.
   * @param orderbyElements ArrayList of columns to order the tuples from the child by.
   * @param bufferPages The amount of main memory space the operator can use.
   */
  public ExternalSortOperator(Operator child, List<Column> orderbyElements, int bufferPages) {
    super(null);

    this.child = child;
    this.orderByElements = new ArrayList<>(orderbyElements);

    indexOrders = new HashMap<>();
    createSortOrder();

    // create directory to work in
    try {
      directory =
          new File(
              Files.createTempDirectory(Paths.get(DBCatalog.getInstance().getSortDirectory()), null)
                  .toString());
    } catch (IOException e) {
      e.printStackTrace();
    }

    this.currTuple = child.getNextTuple();
    this.bufferSize = this.currTuple == null ? 0 : bufferPages * 4096 / (4 * currTuple.size());

    // sort step
    int numSortedRuns = sortBuffer();

    // merge step
    merge(bufferPages - 1, numSortedRuns);

    // create reader for getNextTuple
    try {
      this.readerSortedFile = new TupleReader(sortedFile);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // create output schema
    ArrayList<Column> schema = new ArrayList<>();
    for (int i = 0; i < child.outputSchema.size(); i++) {
      schema.add(child.outputSchema.get(indexOrders.get(i)));
    }
    // set outputSchema
    this.outputSchema = schema;
  }

  public Map<Integer, Integer> getIndexOrders() {
    return indexOrders;
  }

  /**
   * Determines the order to sort the tuples by.
   *
   * @param orderbyElements A List of Columns to sort by first.
   */
  private void createSortOrder() {
    // gives the index in the tuple by order preference (ie. indexOrders[i] = the
    // index in child.outputSchema that is i-th in the final order)
    int position = 0;
    while (position < orderByElements.size()) {
      Column column = orderByElements.get(position);
      for (int i = 0; i < child.outputSchema.size(); i++) {
        Table t = child.outputSchema.get(i).getTable();
        String tableName2 = t.getAlias() != null ? t.getAlias().getName() : t.getName();
        if (child.outputSchema.get(i).getColumnName().equals(column.getColumnName())
            && tableName2.equals(column.getTable().getName())) {
          indexOrders.put(position, i);
          position++;
          break;
        }
      }
    }

    for (int i = 0; i < child.outputSchema.size(); i++) {
      if (!indexOrders.containsValue(i)) {
        indexOrders.put(position, i);
        position++;
      }
    }
  }

  /**
   * Populates the buffer array with tuples from the child.
   *
   * @return whether or not the buffer was able to be filled.
   */
  private boolean fillBuffer() {
    buffer = new ArrayList<>(bufferSize);
    boolean notEmpty = false;
    int i = 0;
    while (i < bufferSize && currTuple != null) {
      buffer.add(i, currTuple);
      currTuple = child.getNextTuple();
      notEmpty = true;
      i++;
    }
    return notEmpty;
  }

  /**
   * Sorts the current buffer using the sort order previously processed.
   *
   * @return the number of sorted runs created.
   */
  private int sortBuffer() {
    int numSortedRuns = 0;
    while (fillBuffer()) {
      buffer.sort(new SortComparator(indexOrders));
      // write buffer
      try {
        TupleWriter tw = new TupleWriter(directory.toString() + "/" + numSortedRuns);
        for (Tuple t : buffer) {
          tw.writeTuple(t);
        }
        tw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      numSortedRuns++;
    }
    return numSortedRuns;
  }

  /**
   * Compares the tuples from each TupleReader and writes them to a temporary file in the desired
   * sort order.
   *
   * @param runs a List of TupleReaders for each sorted run.
   * @param tw the TupleWriter that writes to the temporary file.
   * @throws IOException
   */
  private void mergeHelper(List<TupleReader> runs, TupleWriter tw) throws IOException {
    ArrayList<Tuple> tuples = new ArrayList<>();
    for (TupleReader tr : runs) {
      tuples.add(tr.readNextTuple());
    }
    SortComparator comparator = new SortComparator(indexOrders);
    Tuple smallest = Collections.min(tuples, comparator);
    while (smallest != null) {
      tw.writeTuple(smallest);
      int nextTR = tuples.indexOf(smallest);
      tuples.set(nextTR, runs.get(nextTR).readNextTuple());
      smallest = Collections.min(tuples, comparator);
    }
  }

  /**
   * Combines each of the sorted runs into one file using multiple passes.
   *
   * @param numMerges the number of tuples that can be merged at once.
   * @param numSortedRuns the number of sorted runs created by the sorting step.
   */
  private void merge(int numMerges, int numSortedRuns) {
    int start = 0;
    int end = numSortedRuns;
    while (numSortedRuns > 1) {
      // grab numMerges runs to merge
      // numSortedRuns - numMerges to numSortedRuns-1
      ArrayList<TupleReader> trRuns = new ArrayList<>();
      for (int i = start; i < Math.min(start + numMerges, end); i++) {
        try {
          trRuns.add(new TupleReader(directory.toString() + "/" + i));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      try {
        TupleWriter tw = new TupleWriter(directory.toString() + "/" + end);
        mergeHelper(trRuns, tw);
        for (TupleReader tr : trRuns) {
          tr.close();
        }
        tw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

      // decrease number of sorted runs & update start and end positions
      numSortedRuns -= numMerges - 1;
      start = Math.min(start + numMerges, end);
      end++;
    }
    sortedFile = new File(directory, Integer.toString(start));
  }

  @Override
  public void reset() {
    try {
      readerSortedFile.close();
      readerSortedFile = new TupleReader(sortedFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Tuple getNextTuple() {
    if (readerSortedFile == null) {
      return null;
    }
    try {
      Tuple t = readerSortedFile.readNextTuple();
      if (t == null) {
        readerSortedFile.close();
      }
      return t;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /** returns a string representation of this operator */
  public String toString() {
    return "ExternalSort" + orderByElements;
  }

  /** Returns the list of children belonging to this operator */
  public List<Operator> getChildren() {
    ArrayList<Operator> temp = new ArrayList<>();
    temp.add(child);
    return temp;
  }
}
