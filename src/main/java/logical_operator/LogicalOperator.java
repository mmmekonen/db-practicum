package logical_operator;

import common.PlanBuilder;

/**
 * Abstract class to represent logical operators. Every operator has a reference to an outputSchema
 * which represents the schema of the output tuples from the operator. This is a list of Column
 * objects. Each Column has an embedded Table object with the name and alias (if required) fields
 * set appropriately.
 */
public abstract class LogicalOperator {

  /**
   * Visits the operator in the physical plan builder to create a physical operator from this
   * logical one.
   *
   * @param builder the physical plan builder for the query.
   */
  public abstract void accept(PlanBuilder builder);
}
