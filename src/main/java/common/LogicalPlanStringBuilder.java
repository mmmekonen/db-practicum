package common;

import logical_operator.*;

/** A visitor class to build a string from a logical plan */
public class LogicalPlanStringBuilder extends PlanBuilder {

  StringBuilder plan;
  int depth;

  /** Basic constructor for the object */
  public LogicalPlanStringBuilder() {
    plan = new StringBuilder();
    depth = 0;
  }

  /**
   * toString method that outputs the string in the stringbuilder, which will be the logical plan
   * after the visitor has been accepted/
   *
   * @return the string representation of the last visited logical plan
   */
  public String toString() {
    return plan.toString();
  }

  /**
   * Visits all parts of the operator
   *
   * @param scanOp The operator to be visited
   */
  public void visit(Scan scanOp) {
    for (int i = 0; i < depth; i++) {
      plan.append("-");
    }
    plan.append(scanOp);
  }

  /**
   * Visits all parts of the operator
   *
   * @param selectOp The operator to be visited
   */
  public void visit(Select selectOp) {
    for (int i = 0; i < depth; i++) {
      plan.append("-");
    }
    plan.append(selectOp).append("\n");
    depth++;
    selectOp.getChild().accept(this);
    depth--;
  }

  /**
   * Visits all parts of the operator
   *
   * @param projectionOp The operator to be visited
   */
  public void visit(Projection projectionOp) {
    for (int i = 0; i < depth; i++) {
      plan.append("-");
    }
    plan.append(projectionOp).append("\n");
    depth++;
    projectionOp.getChild().accept(this);
    depth--;
  }

  /**
   * Visits all parts of the operator
   *
   * @param sortOp The operator to be visited
   */
  public void visit(Sort sortOp) {
    for (int i = 0; i < depth; i++) {
      plan.append("-");
    }
    plan.append(sortOp).append("\n");
    depth++;
    sortOp.getChild().accept(this);
    depth--;
  }

  /**
   * Visits all parts of the operator
   *
   * @param duplicateOp The operator to be visited
   */
  public void visit(DuplicateElimination duplicateOp) {
    for (int i = 0; i < depth; i++) {
      plan.append("-");
    }
    plan.append(duplicateOp).append("\n");
    depth++;
    duplicateOp.getChild().accept(this);
    depth--;
  }

  /**
   * Visits all parts of the operator
   *
   * @param joinOp The operator to be visited
   */
  public void visit(Join joinOp) {
    for (int i = 0; i < depth; i++) {
      plan.append("-");
    }
    plan.append(joinOp).append("\n");
    depth++;
    boolean first = true;
    for (LogicalOperator op : joinOp.getChildren()) {
      if (!first) {
        plan.append("\n");
      } else {
        first = false;
      }
      op.accept(this);
    }
    depth--;
  }
}
