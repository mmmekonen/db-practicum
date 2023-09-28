package physical_operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * A class to represent a projection operator on a relation. Projection operator
 * takes in a list of
 * select items and a child operator. It then returns a new operator with the
 * same schema as the
 * child operator, but with only the columns specified in the select items.
 */
public class ProjectionOperator extends Operator {
  private List<Integer> projectionColumnsIndices;
  private Operator child;

  /**
   * Creates a projection operator using an Operator and a List of SelectItems.
   *
   * @param selectItems The select items to project on.
   * @param child       The scan operator's child operator.
   */
  public ProjectionOperator(List<SelectItem> selectItems, Operator child) {
    super(child.outputSchema);
    this.child = child;

    this.projectionColumnsIndices = new ArrayList<>();

    ArrayList<Column> newOutputSchema = new ArrayList<>();

    // get the indices of the columns to project on
    for (SelectItem selectItem : selectItems) {

      // get the column name and table name
      SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
      Column expression = (Column) selectExpressionItem.getExpression();
      String columnName = expression.getColumnName();
      String tableName = expression.getTable().getName();

      int index = 0;

      // finds the column in the child's outputSchema and adds it to the new
      // outputSchema list
      for (Column column : child.outputSchema) {
        Table t = column.getTable();
        String tableName2 = t.getAlias() != null ? t.getAlias().getName() : t.getName();
        if (column.getColumnName().equals(columnName) && tableName2.equals(tableName)) {
          this.projectionColumnsIndices.add(index);
          newOutputSchema.add(column);
        }
        index++;
      }
    }
    this.outputSchema = newOutputSchema;
  }

  /** Resets cursor on the operator to the beginning */
  @Override
  public void reset() {
    child.reset();
  }

  /**
   * Get next tuple from operator Only returns the tuple with the columns
   * specified in the select
   * items projection
   *
   * @return next Tuple, or null if we are at the end
   */
  @Override
  public Tuple getNextTuple() {

    Tuple tuple = child.getNextTuple();
    if (tuple == null) {
      return null;
    }

    // get the values of the columns to project on
    ArrayList<Integer> tupleValues = new ArrayList<>();
    for (int i = 0; i < projectionColumnsIndices.size(); i++) {
      tupleValues.add(tuple.getElementAtIndex(projectionColumnsIndices.get(i)));
    }
    return new Tuple(tupleValues);
  }
}
