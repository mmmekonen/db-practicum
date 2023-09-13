package common;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

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
import java.util.List;
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

  public static HashMap<String, String> alias = new HashMap<String, String>();

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
    // Get the distint keyword
    Distinct distint = plainSelect.getDistinct();
    // get all aliases

    // get all of the joins operators from the statement
    // List<Join> joins = plainSelect.getJoins();

    // if (where == null) {
    // if (selects.get(0) instanceof AllColumns) {
    // return new ScanOperator(table.getName());
    // } else {
    // return new ProjectionOperator(selects, new ScanOperator(table.getName()));
    // }
    // } else {
    // if (selects.get(0) instanceof AllColumns) {
    // return new SelectOperator(table.getName(), new ScanOperator(table.getName()),
    // where);
    // } else {
    // return new ProjectionOperator(selects,
    // new SelectOperator(table.getName(), new ScanOperator(table.getName()),
    // where));
    // }
    // }

    Operator rootOperator;

    if (table.getAlias() != null) {
      alias.put(table.getAlias().getName(), table.getName());
    }

    if (where == null) {
      rootOperator = new ScanOperator(table.getName());
    } else {
      rootOperator = new SelectOperator(table.getName(), new ScanOperator(table.getName()), where);
    }

    // if (joins != null) {
    // for (Join join : joins) {
    // Table joinTable = (Table) join.getRightItem();
    // Expression joinWhere = (Expression) join.getOnExpression();
    // rootOperator = new JoinOperator(table.getName(), (ScanOperator) rootOperator,
    // new ScanOperator(joinTable.getName()), joinWhere);
    // }
    // }

    // if (selects.get(0) instanceof AllColumns) {
    // return rootOperator;
    // } else {
    // return new ProjectionOperator(selects, rootOperator);
    // }

    if (!(selects.get(0) instanceof AllColumns)) {
      rootOperator = new ProjectionOperator(selects, rootOperator);
    }

    List<Column> columns = new ArrayList<>();
    if (orderByElements != null) {
      for (OrderByElement orderByElement : orderByElements) {
        // get the column from the orderByElement
        Column column = (Column) orderByElement.getExpression();
        columns.add(column);
      }
    }

    if (orderByElements != null) {
      System.out.print(orderByElements.size());
      rootOperator = new SortOperator(rootOperator, columns);
    }
    if (distint != null) {
      rootOperator = new DuplicateEliminationOperator(rootOperator);
    }

    return rootOperator;

  }
}
