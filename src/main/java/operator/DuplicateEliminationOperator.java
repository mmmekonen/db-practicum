package operator;

import common.Tuple;

public class DuplicateEliminationOperator extends Operator {

  Operator child;
  Tuple currTuple = null;
  Tuple prevTuple = null;

  public DuplicateEliminationOperator(Operator child) {
    super(null);
    this.child = child;
  }

  @Override
  public void reset() {
    child.reset();
  }

  @Override
  public Tuple getNextTuple() {
    currTuple = child.getNextTuple();
    while (currTuple != null) {
      if (currTuple.equals(prevTuple)) {
        currTuple = child.getNextTuple();
      }
      prevTuple = currTuple;
      return currTuple;
    }
    return null;
  }

}
