package physical_operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import visitors.SelectExpressionVisitor;

/**
 * A class to represent a block-nested loop join. It loads blocks of tuples into
 * the buffer and
 * iterates over them, instead of making an I/O request for each individual
 * tuple.
 */
public class BNLJOperator extends Operator {

  private Operator left;
  private Operator right;
  private Expression expression;
  private Tuple leftTuple;
  private Tuple rightTuple;
  private Tuple[] buffer;
  private int pointer;

  /**
   * Creates a BNLJOperator object that concatenates two other operators together
   * using a
   * tuple-nested loop join.
   *
   * @param left_op    One operator to be joined.
   * @param right_op   Another operator to be joined.
   * @param expression An expression that dictates what combinations of tuples are
   *                   valid.
   */
  public BNLJOperator(Operator leftOp, Operator rightOp, Expression expression, int bufferPages) {
    super(null);
    this.outputSchema = leftOp.getOutputSchema();
    this.outputSchema.addAll(rightOp.getOutputSchema());
    this.left = leftOp;
    this.right = rightOp;
    this.expression = expression;
    this.leftTuple = left.getNextTuple();
    this.rightTuple = right.getNextTuple();
    this.buffer = new Tuple[bufferPages * (4096 - 8) / (4 * leftTuple.size())];
    this.pointer = 0;
    fillBuffer();
  }

  /** Populates the buffer array with tuples from the outer table */
  private void fillBuffer() {
    buffer = new Tuple[buffer.length];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = leftTuple;
      leftTuple = left.getNextTuple();
    }
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

    if (buffer[pointer] == null || rightTuple == null)
      return null;

    Tuple tuple;

    boolean satisfied = false;

    while (buffer[pointer] != null) {
      ArrayList<Integer> combined = buffer[pointer].getAllElements();
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
        //System.out.println(tuple);
        return tuple;
      }

      advance();
    }

    return null;
  }

  /** Helper function to increment the operator */
  private void advance() {
    pointer++;
    if (pointer >= buffer.length || buffer[pointer] == null) {
      pointer = 0;
      rightTuple = right.getNextTuple();
      if (rightTuple == null) {
        right.reset();
        rightTuple = right.getNextTuple();
        fillBuffer();
      }
    }
  }

  public String toString() {
    return "BNLJ[" + expression + "]";
  }

  public List<Operator> getChildren() {
    ArrayList<Operator> temp = new ArrayList<>();
    temp.add(left);
    temp.add(right);
    return temp;
  }
}
