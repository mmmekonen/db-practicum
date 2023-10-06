package common;

import logical_operator.DuplicateElimination;
import logical_operator.Join;
import logical_operator.Projection;
import logical_operator.Scan;
import logical_operator.Select;
import logical_operator.Sort;
import physical_operator.DuplicateEliminationOperator;
import physical_operator.InMemorySortOperator;
import physical_operator.Operator;
import physical_operator.ProjectionOperator;
import physical_operator.ScanOperator;
import physical_operator.SelectOperator;
import physical_operator.TNLJOperator;

/**
 * A class to translate a logical operators into a relational algebra query plan using physical
 * operators. This class uses the visitor pattern to traverse the logical query plan and replaces
 * each logical operator with its corresponding physical operator. The physical operators used
 * depends on the values in the config file.
 */
public class PhysicalPlanBuilder {

  // the root of the physical query plan
  Operator root;

  /** Creates a PhysicalPlanBuilder. */
  public PhysicalPlanBuilder() {}

  /**
   * Returns the root of the physical query plan.
   *
   * @return a Operator.
   */
  public Operator getRoot() {
    return root;
  }

  /**
   * Replaces the logical Scan operator with a physical ScanOperator.
   *
   * @param scanOp a logical Scan operator.
   */
  public void visit(Scan scanOp) {
    root = new ScanOperator(scanOp.getTableName(), scanOp.getAlias());
  }

  /**
   * Replaces the logical Select operator with a physical SelectOperator.
   *
   * @param selectOp a logical Select operator.
   */
  public void visit(Select selectOp) {
    selectOp.getChild().accept(this);

    Operator child = root;
    root = new SelectOperator(child, selectOp.getExpression());
  }

  /**
   * Replaces the logical Projection operator with a physical ProjectionOperator.
   *
   * @param projectionOp a logical Projection operator.
   */
  public void visit(Projection projectionOp) {
    projectionOp.getChild().accept(this);

    Operator child = root;
    root = new ProjectionOperator(projectionOp.getSelectItems(), child);
  }

  /**
   * Replaces the logical Join operator with a physical operator, being either a TNLJ, BNLJ, or SMJ.
   *
   * @param joinOp a logical Join operator.
   */
  public void visit(Join joinOp) {
    joinOp.getLeftChild().accept(this);
    Operator left = root;

    joinOp.getRightChild().accept(this);
    Operator right = root;

    root = new TNLJOperator(left, right, joinOp.getExpression());
  }

  /**
   * Replaces the logical Sort operator with a physical operator for either an in-memory sort or an
   * external sort.
   *
   * @param sortOp a logical Sort operator.
   */
  public void visit(Sort sortOp) {
    sortOp.getChild().accept(this);
    Operator child = root;

    root = new InMemorySortOperator(child, sortOp.getOrderByElements());
  }

  /**
   * Replaces the logical DuplicateElimination operator with a physical
   * DuplicateEliminationOperator.
   *
   * @param duplicateOp a logical DuplicateElimination operator.
   */
  public void visit(DuplicateElimination duplicateOp) {
    duplicateOp.getChild().accept(this);
    Operator child = root;

    root = new DuplicateEliminationOperator(child);
  }
}
