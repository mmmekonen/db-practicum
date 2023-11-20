package logical_operator;

import common.PhysicalPlanBuilder;

/**
 * A class to represent a duplicate elimination operator on a relation. This is
 * the logical operator
 * for the physical operator DuplicateEliminationOperator.
 */
public class DuplicateElimination extends LogicalOperator {

  // the child operator
  private LogicalOperator child;

  /**
   * Creates a logical duplicate elimination operator using a LogicalOperator as
   * its child.
   *
   * @param child The operator's child operator.
   */
  public DuplicateElimination(LogicalOperator child) {
    this.child = child;
  }

  @Override
  public void accept(PhysicalPlanBuilder builder) {
    builder.visit(this);
  }

  /**
   * Returns the child of this operator.
   *
   * @return a logical operator.
   */
  public LogicalOperator getChild() {
    return child;
  }

  /**
   * @return A string representation of the operator
   */
  public String toString() {
    return "DupElim";
  }
}
