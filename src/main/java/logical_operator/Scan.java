package logical_operator;

import net.sf.jsqlparser.expression.Alias;
import common.PhysicalPlanBuilder;

/**
 * A class to represent the a scan operator on a table. This is the logical
 * representation of the
 * physical scan operator, ScanOperator.
 */
public class Scan extends LogicalOperator {

  // the name of the base table the scan is on
  private String tableName;

  // the alias of the base table
  private Alias alias;

  /**
   * Creates a logical scan operator using a table name and an alias.
   *
   * @param tableName The name of the table to scan.
   * @param alias     The alias of the table.
   */
  public Scan(String tableName, Alias alias) {
    this.tableName = tableName;
    this.alias = alias;
  }

  @Override
  public void accept(PhysicalPlanBuilder builder) {
    builder.visit(this);
  }

  /**
   * Returns the name of the base table this scan reads in.
   *
   * @return the table name.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the alias of the base table this scan reads in.
   *
   * @return a JSQLParser Alias.
   */
  public Alias getAlias() {
    return alias;
  }

  /**
   * @return A string representation of the operator
   */
  public String toString() {
    return "Leaf[" + tableName + "]";
  }
}
