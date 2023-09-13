package operator;

import common.QueryPlanBuilder;
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

    this.projectionColumnsIndices = new ArrayList<>();
    for (SelectItem selectItem : selectItems) {
      SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
      Column expression = (Column) selectExpressionItem.getExpression();
      String columnName = expression.getColumnName();
      String tableName = expression.getTable().getName();
      tableName = QueryPlanBuilder.alias.get(tableName) != null ? QueryPlanBuilder.alias.get(tableName) : tableName;

      int index = 0;
      for (Column column : child.outputSchema) {
        if (column.getColumnName().equals(columnName) && column.getTable().getName().equals(tableName)) {
          this.projectionColumnsIndices.add(index);
        }
        index++;
      }
    }
  }

  @Override
  public void reset() {
    child.reset();
  }

  @Override
  public Tuple getNextTuple() {

    Tuple tuple = child.getNextTuple();
    if (tuple == null) {
      return null;
    }

    ArrayList<Integer> tupleValues = new ArrayList<>();
    for (int i = 0; i < projectionColumnsIndices.size(); i++) {
      tupleValues.add(tuple.getElementAtIndex(projectionColumnsIndices.get(i)));
    }
    return new Tuple(tupleValues);
  }
}
