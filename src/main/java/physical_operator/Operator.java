package physical_operator;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import common.Tuple;
import common.TupleWriter;
import net.sf.jsqlparser.schema.Column;

/**
 * Abstract class to represent relational operators. Every operator has a
 * reference to an
 * outputSchema which represents the schema of the output tuples from the
 * operator. This is a list
 * of Column objects. Each Column has an embedded Table object with the name and
 * alias (if required)
 * fields set appropriately.
 */
public abstract class Operator {

  protected ArrayList<Column> outputSchema;

  public Operator(ArrayList<Column> outputSchema) {
    this.outputSchema = outputSchema;
  }

  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }

  /**
   * A function to create a copy of the operator's schema, preventing operators
   * from modifying each other's schema
   * 
   * @return This operator's schema
   */
  public ArrayList<Column> copyOutputSchema() {
    ArrayList<Column> schema = new ArrayList<>();
    schema.addAll(outputSchema);
    return schema;
  }

  /** Resets cursor on the operator to the beginning */
  public abstract void reset();

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public abstract Tuple getNextTuple();

  /**
   * Collects all tuples of this operator.
   *
   * @return A list of Tuples.
   */
  public List<Tuple> getAllTuples() {
    Tuple t;
    List<Tuple> tuples = new ArrayList<>();
    while ((t = getNextTuple()) != null) {
      tuples.add(t);
    }

    return tuples;
  }

  /**
   * Iterate through output of operator and send it all, in Tuple format, to the
   * specified printStream.
   *
   * @param printStream stream to receive output, one tuple per line.
   */
  public void dumpTuples(PrintStream printStream) {
    Tuple t;
    while ((t = this.getNextTuple()) != null) {
      printStream.println(t);
    }
  }

  /**
   * Iterate through output of operator and send it all, in binary format, to the
   * specified printStream.
   *
   * @param file file to receive output, in binary format.
   */
  public void dump(File file) {
    TupleWriter tw;
    try {
      tw = new TupleWriter(file);
      Tuple t;
      while ((t = this.getNextTuple()) != null) {
        tw.writeTuple(t);
      }
      tw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
