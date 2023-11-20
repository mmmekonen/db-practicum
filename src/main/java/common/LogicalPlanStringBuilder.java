package common;

import java.util.ArrayList;
import java.util.HashMap;

import logical_operator.DuplicateElimination;
import logical_operator.Join;
import logical_operator.Projection;
import logical_operator.Scan;
import logical_operator.Select;
import logical_operator.Sort;



public class LogicalPlanStringBuilder extends PlanBuilder {

  StringBuilder plan;
  int depth;

  public LogicalPlanStringBuilder() {
    plan = new StringBuilder();
    depth = 0;
  }

  public String toString() {
    return plan.toString();
  }

  
  public void visit(Scan scanOp) {
    for (int i = 0; i < depth; i++) {
        plan.append("-");
    }
    plan.append(scanOp).append("\n");
  }

  public void visit(Select selectOp) {
    for (int i = 0; i < depth; i++) {
        plan.append("-");
    }
    plan.append(selectOp).append("\n");
    depth++;
    selectOp.getChild().accept(this);
    depth--;
  }

  public void visit(Projection projectionOp) {
    for (int i = 0; i < depth; i++) {
        plan.append("-");
    }
    plan.append(projectionOp).append("\n");
    depth++;
    projectionOp.getChild().accept(this);
    depth--;
  }
  
  public void visit(Sort sortOp) {
    for (int i = 0; i < depth; i++) {
        plan.append("-");
    }
    plan.append(sortOp).append("\n");
    depth++;
    sortOp.getChild().accept(this);
    depth--;
  }

  public void visit(DuplicateElimination duplicateOp) {
    for (int i = 0; i < depth; i++) {
        plan.append("-");
    }
    plan.append(duplicateOp).append("\n");
    depth++;
    duplicateOp.getChild().accept(this);
    depth--;
  }

  public void visit(Join joinOp) {
    for (int i = 0; i < depth; i++) {
        plan.append("-");
    }
    plan.append(joinOp).append("\n");
    depth++;
    joinOp.getLeftChild().accept(this);
    joinOp.getRightChild().accept(this);
    depth--;
  }

}
