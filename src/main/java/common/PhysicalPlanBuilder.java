package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import logical_operator.DuplicateElimination;
import logical_operator.Join;
import logical_operator.LogicalOperator;
import logical_operator.Projection;
import logical_operator.Scan;
import logical_operator.Select;
import logical_operator.Sort;
import net.sf.jsqlparser.expression.Expression;
import physical_operator.*;
import visitors.IndexExpressionSplitter;

/**
 * A class to translate a logical operators into a relational algebra query plan
 * using physical
 * operators. This class uses the visitor pattern to traverse the logical query
 * plan and replaces
 * each logical operator with its corresponding physical operator. The physical
 * operators used
 * depends on the values in the config file.
 */
public class PhysicalPlanBuilder extends PlanBuilder {

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
  JOIN join = JOIN.BNLJ;
  SORT sort = SORT.EXTERNAL;
  int joinBuffer = 5;
  int sortBuffer = 3;
  HashMap<String, HashMap<String, Double>> reductionInfo = new HashMap<>();

  /**
   * Creates a PhysicalPlanBuilder.
   */
  public PhysicalPlanBuilder() {
  }

  /**
   * Creates a PhysicalPlanBuilder with the specified join type.
   * 
   * @param joinType an int value for the join to use.
   */
  public PhysicalPlanBuilder(int joinType) {
    if (joinType == 0)
      this.join = JOIN.TNLJ;
    if (joinType == 1)
      this.join = JOIN.BNLJ;
    if (joinType == 2) // DO NOT FUCK WITH THIS! JUST EDIT THE CONFIG FILE OR THE DEFAULTJOIN FIELD IN
      // QUERYPLANBUILDER!
      this.join = JOIN.SMJ;
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
    Scan scanOp = (Scan) selectOp.getChild();

    // if index exists on table
    DBCatalog db = DBCatalog.getInstance();
    HashMap<String, ArrayList<Integer>> indexInfo = db.getIndexInfo().get(scanOp.getTableName());

    OptimalSelection optimalSelection = new OptimalSelection();

    System.out.println("selectOp.getExpression(): " + selectOp.getExpression());
    System.out.println("scanOp.getTableName(): " + scanOp.getTableName());
    System.out.println("indexInfo: " + indexInfo);

    ArrayList<Object> lowestCost = optimalSelection.getOptimalColumn(selectOp.getExpression(), scanOp.getTableName(),
        indexInfo);

    String columnName = (String) lowestCost.get(0);

    columnName = columnName.split("\\.")[1];
    boolean useIndex = ((double) lowestCost.get(1) == 1.0) ? true : false;

    String tname = (scanOp.getAlias() == null) ? scanOp.getAlias().getName() : scanOp.getTableName();

    this.reductionInfo.put(tname, (HashMap<String, Double>) lowestCost.get(3));

    System.out.println("r info: " + reductionInfo);
    System.out.println("columnName: " + columnName);
    System.out.println("useIndex: " + useIndex);

    if (useIndex) {
      IndexExpressionSplitter splitter = new IndexExpressionSplitter(columnName);
      selectOp.getExpression().accept(splitter);

      // Expression indexExpression = splitter.getIndexConditions();
      Integer lowkey = splitter.getLowKey();
      Integer highkey = splitter.getHighKey();
      Expression selectExpression = splitter.getSelectConditions();

      if (lowkey == null && highkey == null) {
        // full scan
        root = new SelectOperator(
            new ScanOperator(scanOp.getTableName(), scanOp.getAlias()), selectExpression);
      } else if (selectExpression == null) {
        // only indexScan operator, no selection
        root = new IndexScanOperator(
            scanOp.getTableName(), scanOp.getAlias(), columnName, lowkey, highkey);
      } else {
        // both indexscan and selection
        Operator indexScanOp = new IndexScanOperator(
            scanOp.getTableName(), scanOp.getAlias(), columnName, lowkey, highkey);
        root = new SelectOperator(indexScanOp, selectExpression);
      }
    } else {
      scanOp.accept(this);
      Operator child = root;
      root = new SelectOperator(child, selectOp.getExpression());
    }
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
    List<LogicalOperator> children = joinOp.getChildren();
    ArrayList<Operator> opChildren = new ArrayList<>();
    for (LogicalOperator child : children) {
      child.accept(this);
      opChildren.add(root);
    }

    DetermineJoinOrder determineOrder = new DetermineJoinOrder(opChildren, null/* TODO: Put select uf here */,
        reductionInfo);
    ArrayList<Operator> order = determineOrder.finalOrder();
    Operator left = order.get(0);
    for (int i = 1; i < order.size(); i++) {
      left = new BNLJOperator(left, order.get(i), null/** TODO: put expression here */
          , joinBuffer);
    }
    root = left;

    // old code

    // if (join == JOIN.TNLJ) {
    // // System.out.println("TNLJ");
    // root = new TNLJOperator(left, right, joinOp.getExpression());
    // }
    // if (join == JOIN.BNLJ) {
    // // System.out.println("BNLJ");
    // root = new BNLJOperator(left, right, joinOp.getExpression(), joinBuffer);
    // }
    // if (join == JOIN.SMJ) {
    // unimplemented

    // ArrayList<Integer> leftOrder = new ArrayList<>();
    // ArrayList<Integer> rightOrder = new ArrayList<>();
    // ArrayList<Column> rightOrderCols = new ArrayList<>();
    // ArrayList<Column> leftOrderCols = new ArrayList<>();

    // ExpressionVisitor visitorL = new
    // OrderByElementExtractor(left.getOutputSchema());
    // joinOp.getExpression().accept(visitorL);
    // leftOrder = ((OrderByElementExtractor) visitorL).getOrderByElements();
    // leftOrderCols = ((OrderByElementExtractor)
    // visitorL).getOrderByElementsColumns();

    // ExpressionVisitor visitorR = new
    // OrderByElementExtractor(right.getOutputSchema());
    // joinOp.getExpression().accept(visitorR);
    // rightOrder = ((OrderByElementExtractor) visitorR).getOrderByElements();
    // rightOrderCols = ((OrderByElementExtractor)
    // visitorR).getOrderByElementsColumns();

    // // System.out.println("");

    // // System.out.println(joinOp.getExpression().toString());
    // // System.out.println(leftOrder);
    // // System.out.println(rightOrder);
    // // System.out.println(leftOrderCols);
    // // System.out.println(rightOrderCols);
    // // System.out.println(right);
    // // System.out.println(left);
    // // System.out.println(left.outputSchema);
    // // System.out.println(right.outputSchema);

    // // System.out.println("");

    // if (sort == SORT.IN_MEMORY) {
    // // if (left.outputSchema != null)
    // left = new InMemorySortOperator(left, leftOrderCols);
    // // if (right.outputSchema != null)
    // right = new InMemorySortOperator(right, rightOrderCols);
    // }
    // if (sort == SORT.EXTERNAL) {
    // // if (left.outputSchema != null)
    // left = new ExternalSortOperator(left, leftOrderCols, sortBuffer);
    // // if (right.outputSchema != null)
    // right = new ExternalSortOperator(right, rightOrderCols, sortBuffer);
    // }

    // root = new SMJOperator(joinOp.getExpression(), left, right, leftOrder,
    // rightOrder);

    // // System.out.println("");
    // }

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
