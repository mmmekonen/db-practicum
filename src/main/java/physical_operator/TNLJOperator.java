package physical_operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import visitors.SelectExpressionVisitor;

/**
 * A class to represent a tuple-nested loop join. It loops over each tuple in the inner and outer
 * tables and only returns tuples that satisfy the given expression.
 */
public class TNLJOperator extends Operator {

  private Operator left;
  private Operator right;
  private Expression expression;
  private Tuple leftTuple;
  private Tuple rightTuple;

  /**
   * Creates a TNLJOperator object that concatenates two other operators together using a
   * tuple-nested loop join.
   *
   * @param left_op One operator to be joined.
   * @param right_op Another operator to be joined.
   * @param expression An expression that dictates what combinations of tuples are valid.
   */
  public TNLJOperator(Operator left_op, Operator right_op, Expression expression) {
    super(null);
    this.outputSchema = left_op.getOutputSchema();
    this.outputSchema.addAll(right_op.getOutputSchema());
    this.left = left_op;
    this.right = right_op;
    this.expression = expression;
    this.leftTuple = left.getNextTuple();
    this.rightTuple = right.getNextTuple();
  }

  /** Resets cursor on the operator to the beginning */
  public void reset() {
    left.reset();
    right.reset();
  }

  /**
   * Returns the next valid combination of tuples from the child operators
   *
   * @return next Tuple, or null if we are at the end
   */
  public Tuple getNextTuple() {

    if (leftTuple == null || rightTuple == null) return null;

    Tuple tuple;

    boolean satisfied = false;

    while (!satisfied && leftTuple != null) {
      ArrayList<Integer> combined = leftTuple.getAllElements();
      combined.addAll(rightTuple.getAllElements());
      tuple = new Tuple(combined);

      if (expression != null) {
        SelectExpressionVisitor visitor = new SelectExpressionVisitor(tuple, outputSchema);
        expression.accept(visitor);
        satisfied = visitor.conditionSatisfied();
      } else {
        satisfied = true;
      }
      if (satisfied) {
        advance();
        return tuple;
      }

      advance();
    }

    return null;
  }

  /** Helper function to increment the operator */
  private void advance() {
    rightTuple = right.getNextTuple();
    if (rightTuple == null) {
      right.reset();
      rightTuple = right.getNextTuple();
      leftTuple = left.getNextTuple();
    }
  }

  /** returns a string representation of this operator */
  public String toString() {
    return "TNLJ[" + expression + "]";
  }

  /** Returns the list of children belonging to this operator */
  public List<Operator> getChildren() {
    ArrayList<Operator> temp = new ArrayList<>();
    temp.add(left);
    temp.add(right);
    return temp;
  }
}
