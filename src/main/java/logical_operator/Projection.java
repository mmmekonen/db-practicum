package logical_operator;

import java.util.List;
import net.sf.jsqlparser.statement.select.SelectItem;
import common.PhysicalPlanBuilder;

/**
 * A class to represent a projection operator on a relation. This is the logical
 * representation of
 * the physical projection operator, ProjectionOperator.
 */
public class Projection extends LogicalOperator {

  // the select items for columns to keep
  private List<SelectItem> selectItems;

  // the child of this operator
  private LogicalOperator child;

  /**
   * Creates a logical projection operator using a LogicalOperator and a List of
   * SelectItems.
   *
   * @param selectItems The select items to project on.
   * @param child       The scan operator's child operator.
   */
  public Projection(List<SelectItem> selectItems, LogicalOperator child) {
    this.selectItems = selectItems;
    this.child = child;
  }

  @Override
  public void accept(PhysicalPlanBuilder builder) {
    builder.visit(this);
  }

  /**
   * Returns a list of select items for the projection.
   *
   * @return a list of SelectItem.
   */
  public List<SelectItem> getSelectItems() {
    return selectItems;
  }

  /**
   * Returns the logical child operator of this operator.
   *
   * @return a logical operator.
   */
  public LogicalOperator getChild() {
    return child;
  }
}
