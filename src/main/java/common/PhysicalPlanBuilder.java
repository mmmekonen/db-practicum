package common;

import logical_operator.DuplicateElimination;
import logical_operator.Join;
import logical_operator.Projection;
import logical_operator.Scan;
import logical_operator.Select;
import logical_operator.Sort;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import physical_operator.*;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import common.SortComparator;
import java.util.HashMap;
import java.util.List;

/**
 * A class to translate a logical operators into a relational algebra query plan
 * using physical
 * operators. This class uses the visitor pattern to traverse the logical query
 * plan and replaces
 * each logical operator with its corresponding physical operator. The physical
 * operators used
 * depends on the values in the config file.
 */
public class PhysicalPlanBuilder {

  enum JOIN {
    TNLJ,
    BNLJ,
    SMJ
  }

  enum SORT {
    IN_MEMORY,
    EXTERNAL
  }

  // the root of the physical query plan
  Operator root;
  JOIN join;
  SORT sort;
  int joinBuffer;
  int sortBuffer;

  /** Creates a PhysicalPlanBuilder. */
  public PhysicalPlanBuilder(int joinType, int joinBuffer, int sortType, int sortBuffer) {

    System.out.print("Join Type: " + joinType);
    System.out.println(", Sort Type: " + joinType);

    if (joinType == 0)
      this.join = JOIN.TNLJ;
    if (joinType == 1)
      this.join = JOIN.BNLJ;
    if (joinType == 0)
      this.join = JOIN.SMJ;

    if (sortType == 0)
      this.sort = SORT.IN_MEMORY;
    if (sortType == 1)
      this.sort = SORT.EXTERNAL;

    this.joinBuffer = joinBuffer;
    this.sortBuffer = sortBuffer;
  }

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
   * Replaces the logical Join operator with a physical operator, being either a
   * TNLJ, BNLJ, or SMJ.
   *
   * @param joinOp a logical Join operator.
   */
  public void visit(Join joinOp) {
    joinOp.getLeftChild().accept(this);
    Operator left = root;
    joinOp.getRightChild().accept(this);
    Operator right = root;

    if (join == JOIN.TNLJ) {
      System.out.println("TNLJ");
      root = new TNLJOperator(left, right, joinOp.getExpression());
    }
    if (join == JOIN.BNLJ) {
      System.out.println("BNLJ");
      root = new BNLJOperator(left, right, joinOp.getExpression(), joinBuffer);
    }
    if (join == JOIN.SMJ) {
      System.out.println("SMJ");

      ArrayList<Integer> leftOrder = new ArrayList<>();
      ArrayList<Integer> rightOrder = new ArrayList<>();
      ArrayList<Column> rightOrderCols = new ArrayList<>();
      ArrayList<Column> leftOrderCols = new ArrayList<>();

      ExpressionVisitor visitorL = new OrderByElementExtractor(left.getOutputSchema());
      joinOp.getExpression().accept(visitorL);
      leftOrder = ((OrderByElementExtractor) visitorL).getOrderByElements();
      leftOrderCols = ((OrderByElementExtractor) visitorL).getOrderByElementsColumns();

      ExpressionVisitor visitorR = new OrderByElementExtractor(right.getOutputSchema());
      joinOp.getExpression().accept(visitorR);
      rightOrder = ((OrderByElementExtractor) visitorR).getOrderByElements();
      rightOrderCols = ((OrderByElementExtractor) visitorR).getOrderByElementsColumns();

      // System.out.println("");

      // System.out.println(joinOp.getExpression().toString());
      // System.out.println(leftOrder);
      // System.out.println(rightOrder);
      // System.out.println(leftOrderCols);
      // System.out.println(rightOrderCols);
      // System.out.println(right);
      // System.out.println(left);
      // System.out.println(left.outputSchema);
      // System.out.println(right.outputSchema);

      // System.out.println("");

      if (sort == SORT.IN_MEMORY) {
        // if (left.outputSchema != null)
        left = new InMemorySortOperator(left, leftOrderCols);
        // if (right.outputSchema != null)
        right = new InMemorySortOperator(right, rightOrderCols);
      }
      if (sort == SORT.EXTERNAL) {
        // if (left.outputSchema != null)
        left = new ExternalSortOperator(left, leftOrderCols, sortBuffer);
        // if (right.outputSchema != null)
        right = new ExternalSortOperator(right, rightOrderCols, sortBuffer);
      }

      root = new SMJOperator(joinOp.getExpression(), left, right, leftOrder, rightOrder);

      // System.out.println("");
    }
  }

  /**
   * Replaces the logical Sort operator with a physical operator for either an
   * in-memory sort or an
   * external sort.
   *
   * @param sortOp a logical Sort operator.
   */
  public void visit(Sort sortOp) {
    sortOp.getChild().accept(this);
    Operator child = root;

    if (sort == SORT.IN_MEMORY)
      root = new InMemorySortOperator(child, sortOp.getOrderByElements());
    if (sort == SORT.EXTERNAL)
      root = new ExternalSortOperator(child, sortOp.getOrderByElements(), sortBuffer);

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
