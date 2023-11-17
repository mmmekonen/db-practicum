package logical_operator;

import net.sf.jsqlparser.expression.Expression;
import common.PhysicalPlanBuilder;

/**
 * A class to represent a join. This is the logical representation of the
 * physical join operator.
 * The physical join operator can either be a tuple-nested loop join, a
 * block-nested loop join, or a
 * sort-merge join.
 */
public class Join extends LogicalOperator {

  // the logical left child operator
  private LogicalOperator leftChild;

  // the logical right child operator
  private LogicalOperator rightChild;

  // the expression to check if the tuples should be joined
  private Expression expression;

  /**
   * Creates a logical join operator that concatenates two other operators
   * together using a
   * tuple-nested loop join.
   *
   * @param left_op    One logical operator to be joined.
   * @param right_op   Another logical operator to be joined.
   * @param expression An expression that dictates what combinations of tuples are
   *                   valid.
   */
  public Join(LogicalOperator left_op, LogicalOperator right_op, Expression expression) {
    this.leftChild = left_op;
    this.rightChild = right_op;
    this.expression = expression;
  }

  @Override
  public void accept(PhysicalPlanBuilder builder) {
    builder.visit(this);
  }

  /**
   * Returns the left logical child operator of this operator.
   *
   * @return a logical operator.
   */
  public LogicalOperator getLeftChild() {
    return leftChild;
  }

  /**
   * Returns the right logical child operator of this operator.
   *
   * @return a logical operator.
   */
  public LogicalOperator getRightChild() {
    return rightChild;
  }

  /**
   * Returns the expression of this operator.
   *
   * @return a JSQLParser Expression.
   */
  public Expression getExpression() {
    return expression;
  }
}
