package physical_operator;

import java.util.ArrayList;
import common.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class SMJOperator extends Operator {

  public Tuple leftTuple;
  public Tuple rightTuple;
  public ArrayList<Integer> leftSortOrder;
  public ArrayList<Integer> rightSortOrder;
  public Operator left;
  public Operator right;
  public int leftIndex;
  public int rightIndex;
  public int index;

  /**
   * Constructor for the SMJ operator
   * 
   * @param expression     The expression to be evaluated
   * @param left           The left child operator
   * @param right          The right child operator
   * @param leftSortOrder  The sort order for the left child
   * @param rightSortOrder The sort order for the right child
   */
  public SMJOperator(Expression expression, Operator left, Operator right, ArrayList<Integer> leftSortOrder,
      ArrayList<Integer> rightSortOrder) {
    super(null);
    this.left = left;
    this.right = right;
    this.leftTuple = left.getNextTuple();
    this.rightTuple = right.getNextTuple();
    this.leftSortOrder = leftSortOrder;
    this.rightSortOrder = rightSortOrder;
    this.leftIndex = 0;
    this.rightIndex = 0;
    this.index = -1;
  }

  /**
   * Compares two tuples based on the sort order
   * 
   * @param left
   * @param right
   * @return -1 if left < right, 0 if left == right, 1 if left > right
   */
  public int compare(Tuple left, Tuple right) {
    for (int i = 0; i < leftSortOrder.size(); i++) {
      int leftVal = left.getElementAtIndex(leftSortOrder.get(i));
      int rightVal = right.getElementAtIndex(rightSortOrder.get(i));

      if (leftVal < rightVal) {
        return -1;
      } else if (leftVal > rightVal) {
        return 1;
      }
    }

    return 0;
  }

  /**
   * Resets the operator
   */
  @Override
  public void reset() {
    left.reset();
    right.reset();
    leftTuple = left.getNextTuple();
    rightTuple = right.getNextTuple();
    leftIndex = 0;
    rightIndex = 0;
    index = -1;
  }

  /**
   * Returns the next valid combination of tuples from the child operators
   * 
   * @return next Tuple, or null if we are at the end
   */
  @Override
  public Tuple getNextTuple() {
    Tuple result = null;

    while (leftTuple != null && rightTuple != null) {
      if (index != -1) {
        while (compare(leftTuple, rightTuple) == -1) {
          leftTuple = left.getNextTuple();
          leftIndex++;
        }

        while (compare(leftTuple, rightTuple) == 1) {
          rightTuple = right.getNextTuple();
          rightIndex++;
        }

        index = rightIndex;
      }
      if (compare(leftTuple, rightTuple) == 0) {
        result = new Tuple(mergeTuples(leftTuple, rightTuple));
        rightTuple = right.getNextTuple();
        rightIndex++;
        return result;
      } else {
        rightTuple = right.reset(index);
        leftTuple = left.getNextTuple();
        leftIndex++;
        index = -1;
      }

    }

    return null;
  }

  /**
   * Merges two tuples
   * 
   * @param left
   * @param right
   * @return
   */
  public ArrayList<Integer> mergeTuples(Tuple leftTup, Tuple rightTup) {
    ArrayList<Integer> mergedTuple = new ArrayList<Integer>();
    mergedTuple.addAll(rightTup.getAllElements());
    mergedTuple.addAll(leftTup.getAllElements());
    return mergedTuple;
  }
}