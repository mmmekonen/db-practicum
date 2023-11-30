package physical_operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.List;

/**
 * A class to represent a duplicate elimination operator on a relation. Eliminates duplicate tuples
 * from the relation.
 */
public class DuplicateEliminationOperator extends Operator {

  Operator child;
  Tuple currTuple = null;
  Tuple prevTuple = null;

  /**
   * Creates a duplicate elimination operator using an Operator.
   *
   * @param child The operator's child operator.
   */
  public DuplicateEliminationOperator(Operator child) {
    super(child.outputSchema);
    this.child = child;
  }

  /** Resets cursor on the operator to the beginning */
  @Override
  public void reset() {
    child.reset();
  }

  /**
   * Iterates through the tuples from the child operator and returns the next tuple that is not a
   * duplicate.
   *
   * @return The next tuple that is not a duplicate.
   */
  @Override
  public Tuple getNextTuple() {
    currTuple = child.getNextTuple();
    while (currTuple != null) {
      if (!currTuple.equals(prevTuple)) {
        prevTuple = currTuple;
        return currTuple;
      }
      currTuple = child.getNextTuple();
    }
    return null;
  }

  public String toString() {
    return "DupElim";
  }

  public List<Operator> getChildren() {
    ArrayList<Operator> temp = new ArrayList<>();
    temp.add(child);
    return temp;
  }
}
