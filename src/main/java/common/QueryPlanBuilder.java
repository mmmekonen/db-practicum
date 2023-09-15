package common;

import java.lang.reflect.Array;
import java.util.*;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.schema.Column;
import operator.*;

/**
 * Class to translate a JSQLParser statement into a relational algebra query
 * plan. For now only
 * works for Statements that are Selects, and specifically PlainSelects. Could
 * implement the visitor
 * pattern on the statement, but doesn't for simplicity as we do not handle
 * nesting or other complex
 * query features.
 *
 * <p>
 * Query plan fixes join order to the order found in the from clause and uses a
 * left deep tree
 * join. Maximally pushes selections on individual relations and evaluates join
 * conditions as early
 * as possible in the join tree. Projections (if any) are not pushed and
 * evaluated in a single
 * projection operator after the last join. Finally, sorting and duplicate
 * elimination are added if
 * needed.
 *
 * <p>
 * For the subset of SQL which is supported as well as assumptions on semantics,
 * see the Project
 * 2 student instructions, Section 2.1
 */
public class QueryPlanBuilder {

  public QueryPlanBuilder() {
  }

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
    // Get the order by items
    List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
    // Get the distinct keyword
    Distinct distinct = plainSelect.getDistinct();
    List<Join> joins = plainSelect.getJoins();

    Operator rootOperator;
    if (joins == null) rootOperator = selectHelper(table, where);
    else rootOperator = joinHelper(table, joins, where);
    rootOperator = projectionHelper(rootOperator, selects);
    rootOperator = sortHelper(rootOperator, orderByElements);
    rootOperator = distinctHelper(rootOperator, distinct);
    return rootOperator;

  }

  private Operator selectHelper(Table table, Expression where) {
    if (where == null) {
      return new ScanOperator(table.getName(), table.getAlias());
    } else {
      return new SelectOperator(new ScanOperator(table.getName(), table.getAlias()), where);
    }
  }

  private Operator projectionHelper(Operator child, ArrayList selects) {
    if (!(selects.get(0) instanceof AllColumns)) {
      return new ProjectionOperator(selects, child);
    } else return child;
  }

  private Operator sortHelper(Operator child, List<OrderByElement> orderByElements) {
    ArrayList<Column> columns = new ArrayList<>();
    if (orderByElements != null) {
      for (OrderByElement orderByElement : orderByElements) {
        // get the column from the orderByElement
        Column column = (Column) orderByElement.getExpression();
        columns.add(column);
      }
      return new SortOperator(child, columns);
    } else return child;
  }

  private Operator joinHelper(Table original, List<Join> joins, Expression where) {

    ExpressionSplitter e = new ExpressionSplitter();
    where.accept(e);

    Operator root = selectHelper(original, e.getConditions(original));
    ArrayList schema = DBCatalog.getInstance().getTableSchema(original.getName());

    for(int i = 0; i < joins.size(); i++) {
      Table joinTable = (Table) joins.get(i).getRightItem();
      schema.addAll(DBCatalog.getInstance().getTableSchema(joinTable.getName()));

      root = new JoinOperator(schema, root, selectHelper(joinTable, e.getConditions(joinTable)), where);
    }

    return root;
  }

  private Operator distinctHelper(Operator child, Distinct distinct) {
    if (distinct != null) {
      return new DuplicateEliminationOperator(child);
    } else return child;
  }

}
