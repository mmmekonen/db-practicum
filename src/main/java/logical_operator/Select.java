package logical_operator;

import net.sf.jsqlparser.expression.Expression;
import common.PhysicalPlanBuilder;

/**
 * A class to represent a select operator on a relation. This is the logical
 * representation of the
 * physical select operator, SelectOperator.
 */
public class Select extends LogicalOperator {

  // the operator's child operator
  private LogicalOperator child;

  // the expression this operator applies to each tuple
  private Expression expression;

  /**
   * Creates a logical select operator using a LogicalOperator as its child and an
   * Expression to
   * evaluate each tuple by.
   *
   * @param child      The select operator's logical child operator.
   * @param expression Expression from the WHERE clause of the query.
   */
  public Select(LogicalOperator child, Expression expression) {
    this.child = child;
    this.expression = expression;
  }

  /**
   * Returns the logical operator of this operator's child.
   *
   * @return the logical child operator.
   */
  public LogicalOperator getChild() {
    return child;
  }

  /**
   * Returns the expression the select operator uses to filter tuples.
   *
   * @return a JSQLParser Expression.
   */
  public Expression getExpression() {
    return expression;
  }

  @Override
  public void accept(PhysicalPlanBuilder builder) {
    builder.visit(this);
  }
}
