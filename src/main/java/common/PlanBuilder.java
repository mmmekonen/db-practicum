package common;

import logical_operator.*;


public abstract class PlanBuilder {

  public abstract void visit(Scan scanOp);

  public abstract void visit(Select selectOp);

  public abstract void visit(Projection projectionOp);
  
  public abstract void visit(Sort sortOp);

  public abstract void visit(DuplicateElimination duplicateOp);

  public abstract void visit(Join joinOp);


}
