package operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOperator extends Operator {
  private List<Integer> projectionColumnsIndices;
  private Operator child;

  public ProjectionOperator(List<SelectItem> selectItems, Operator child) {
    super(child.outputSchema);
    this.child = child;
    if (selectItems.size() > 0 && selectItems.get(0) instanceof AllColumns) {
      this.projectionColumnsIndices = new ArrayList<>();
      for (int i = 0; i < child.outputSchema.size(); i++) {
        this.projectionColumnsIndices.add(i);
      }
    } else {
      this.projectionColumnsIndices = new ArrayList<>();
      for (SelectItem selectItem : selectItems) {
        SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
        Column expression = (Column) selectExpressionItem.getExpression();
        String columnName = expression.getColumnName();

        int index = 0;
        for (Column column : child.outputSchema) {
          if (column.getColumnName().equals(columnName)) {
            this.projectionColumnsIndices.add(index);
          }
          index++;
        }
      }
    }
  }

  @Override
  public void reset() {
    child.reset();
  }

  @Override
  public Tuple getNextTuple() {
    // System.out.println("in get next tuple");

    Tuple tuple = child.getNextTuple();
    if (tuple == null) {
      return null;
    }

    // for (int i = 0; i < projectionColumnsIndices.size(); i++) {
    //   System.out.println(projectionColumnsIndices.get(i));
    // }

    ArrayList<Integer> tupleValues = new ArrayList<>();
    for (int i = 0; i < projectionColumnsIndices.size(); i++) {
      tupleValues.add(tuple.getElementAtIndex(projectionColumnsIndices.get(i)));
    }
    return new Tuple(tupleValues);
  }
}
