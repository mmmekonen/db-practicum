package physical_operator;

import common.Tuple;
import net.sf.jsqlparser.expression.Expression;
import visitors.SelectExpressionVisitor;

/**
 * A class to represent a select operator on a relation. Takes a tuple from its child operator and
 * returns it if it satisfies the respective predicate in the WHERE clause.
 */
public class SelectOperator extends Operator {

  // the operator's child operator
  private Operator child;

  // the expression this operator applies to each tuple
  private Expression expression;

  /**
   * Creates a select operator using an Operator as its child and an Expression to evaluate each
   * tuple by.
   *
   * @param child The select operator's child operator.
   * @param expression Expression from the WHERE clause of the query.
   */
  public SelectOperator(Operator child, Expression expression) {
    super(child.outputSchema);
    this.child = child;
    this.expression = expression;
  }

  /** Resets the cursor to the beginning of the operator */
  public void reset() {
    child.reset();
  }

  /**
   * Iterates over the child operator until it finds a tuple that matches the conditions specified
   * in the expression
   *
   * @return The next tuple that matches the expression
   */
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
