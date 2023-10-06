package physical_operator;

import common.SortComparator;
import common.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * A class to represent an in-memory sort operator on a relation. Sorts each tuple in the order of
 * columns in orderbyElements and then in the order of the remaining columns in it's child
 * operator's outputSchema.
 */
public class InMemorySortOperator extends Operator {

  // all of the tuples from the child
  private List<Tuple> tuples;

  // the index of the next tuple to get
  private int index = 0;

  /**
   * Creates a sort operator using an Operator and a List of Columns to order by.
   *
   * @param child The scan operator's child operator.
   * @param orderbyElements ArrayList of columns to order the tuples from the child by.
   */
  public InMemorySortOperator(Operator child, List<Column> orderbyElements) {
    super(null);

    // get all child tuples
    this.tuples = child.getAllTuples();

    // gives the index in the tuple by order preference (ie. indexOrders[i] = the
    // index in child.outputSchema that is i-th in the final order)
    HashMap<Integer, Integer> indexOrders = new HashMap<>();
    int position = 0;
    while (position < orderbyElements.size()) {
      Column column = orderbyElements.get(position);
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

    // sort the tuples
    this.tuples.sort(new SortComparator(indexOrders));

    // create output schema
    ArrayList<Column> schema = new ArrayList<>();
    for (int i = 0; i < child.outputSchema.size(); i++) {
      schema.add(child.outputSchema.get(indexOrders.get(i)));
    }
    // set outputSchema
    this.outputSchema = schema;
  }

  /** Resets cursor on the operator to the beginning */
  @Override
  public void reset() {
    this.index = 0;
  }

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  @Override
  public Tuple getNextTuple() {
    while (this.tuples.size() > this.index) {
      this.index++;
      return this.tuples.get(this.index - 1);
    }
    return null;
  }
}
