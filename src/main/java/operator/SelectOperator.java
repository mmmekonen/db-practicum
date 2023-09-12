package operator;

import common.DBCatalog;
import common.SelectExpressionVisitor;
import common.Tuple;
import net.sf.jsqlparser.expression.Expression;

/** TODO: */
public class SelectOperator extends Operator {

  private ScanOperator child;
  private Expression expression;

  /** TODO: */
  public SelectOperator(String tablename, ScanOperator child, Expression expression) {
    // change
    super(DBCatalog.getInstance().getTableSchema(tablename));

    this.child = child;
    this.expression = expression;
  }

  @Override
  public void reset() {
    child.reset();
  }

  @Override
  public Tuple getNextTuple() {
    Tuple tuple = child.getNextTuple();
    SelectExpressionVisitor visitor = new SelectExpressionVisitor(tuple, child.outputSchema);
    boolean satisfied = false;
    while (!satisfied) {
      if (tuple == null) {
        return tuple;
      }
      expression.accept(visitor);
      satisfied = visitor.conditionSatisfied();
      if (!satisfied) {
        tuple = child.getNextTuple();
        visitor = new SelectExpressionVisitor(tuple, child.outputSchema);
      }
    }
    return tuple;
  }
}
