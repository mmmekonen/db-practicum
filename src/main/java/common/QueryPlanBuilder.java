package common;

import java.util.ArrayList;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import operator.*;

/**
 * Class to translate a JSQLParser statement into a relational algebra query plan. For now only
 * works for Statements that are Selects, and specifically PlainSelects. Could implement the visitor
 * pattern on the statement, but doesn't for simplicity as we do not handle nesting or other complex
 * query features.
 *
 * <p>Query plan fixes join order to the order found in the from clause and uses a left deep tree
 * join. Maximally pushes selections on individual relations and evaluates join conditions as early
 * as possible in the join tree. Projections (if any) are not pushed and evaluated in a single
 * projection operator after the last join. Finally, sorting and duplicate elimination are added if
 * needed.
 *
 * <p>For the subset of SQL which is supported as well as assumptions on semantics, see the Project
 * 2 student instructions, Section 2.1
 */
public class QueryPlanBuilder {
  public QueryPlanBuilder() {}

  /**
   * Top level method to translate statement to query plan
   *
   * @param stmt statement to be translated
   * @return the root of the query plan
   * @precondition stmt is a Select having a body that is a PlainSelect
   */
  public Operator buildPlan(Statement stmt) {
    Select select = (Select) stmt;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    Table table = (Table) plainSelect.getFromItem();
    Expression where = (Expression) plainSelect.getWhere();
    ArrayList<SelectItem> selects = (ArrayList<SelectItem>) plainSelect.getSelectItems();

    if (where == null && selects.size() == 0) {
      return new ScanOperator(table.getName());
    }
    if (where == null && selects.size() > 0) {
      return new ProjectionOperator(selects, new ScanOperator(table.getName()));
    }
    if (where != null && selects.size() == 0) {
      return new SelectOperator(table.getName(), new ScanOperator(table.getName()), where);
    }
    return new ProjectionOperator(
        selects, new SelectOperator(table.getName(), new ScanOperator(table.getName()), where));
  }
}
