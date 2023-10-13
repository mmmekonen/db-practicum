package common;

import java.util.*;
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
    List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
    Distinct distinct = plainSelect.getDistinct();
    List<Join> joins = plainSelect.getJoins();

    Operator rootOperator;
    if (joins == null)
      rootOperator = selectHelper(table, where);
    else
      rootOperator = joinHelper(table, joins, where);
    rootOperator = projectionHelper(rootOperator, selects);
    rootOperator = sortHelper(rootOperator, orderByElements, distinct);
    rootOperator = distinctHelper(rootOperator, distinct);
    return rootOperator;
  }

  /**
   * Helper function to create a SelectOperator object iff necessary, or a
   * ScanOperator if that would suffice
   * 
   * @param table The table specified in stmnt
   * @param where The conditions specified in stmnt
   * @return An operator that returns the next tuple in the table that matches the
   *         specified conditions
   */
  private Operator selectHelper(Table table, Expression where) {
    if (where == null) {
      return new ScanOperator(table.getName(), table.getAlias());
    } else {
      return new SelectOperator(new ScanOperator(table.getName(), table.getAlias()), where);
    }
  }

  /**
   * Helper function that creates a ProjectionOperator object iff projection is
   * required by the statement
   * 
   * @param child   A child from whose tuples this operator will project
   * @param selects The conditions for projection
   * @return An operator that returns a projection of the next tuple in the table
   */
  private Operator projectionHelper(Operator child, ArrayList<SelectItem> selects) {
    if (!(selects.get(0) instanceof AllColumns)) {
      return new ProjectionOperator(selects, child);
    } else
      return child;
  }

  /**
   * Helper function that creates a SortOperator object to sort the table by a set
   * of columns, or sorts them according
   * to the schema if the statement calls for distinct values (as this depends on
   * a sorted table)
   * 
   * @param child           A child whose tuples will be sorted
   * @param orderByElements The elements by which the tuples will be sorted
   * @param distinct        If this value is not null, the table will be sorted
   *                        according to its schema
   * @return An operator that sorts the table according to the statement
   */
  private Operator sortHelper(Operator child, List<OrderByElement> orderByElements, Distinct distinct) {
    ArrayList<Column> columns = new ArrayList<>();
    if (orderByElements != null) {
      for (OrderByElement orderByElement : orderByElements) {
        Column column = (Column) orderByElement.getExpression();
        columns.add(column);
      }
      return new SortOperator(child, columns);
    } else if (distinct != null) {
      return new SortOperator(child, new ArrayList<Column>(0));
    } else {
      return child;
    }
  }

  /**
   * A helper function that creates nested JoinOperators that join together all
   * tables specified in the statement
   * 
   * @param original The first table specified in the statement
   * @param joins    The tables to be joined onto the original table
   * @param where    An expression that specifies the conditions by which to join
   *                 the tables together
   * @return A JoinOperator that returns the next tuple in the joined tables
   */

  private Operator joinHelper(Table original, List<Join> joins, Expression where) {

    if (where == null) {
      Operator root = selectHelper(original, null);

      for (int i = 0; i < joins.size(); i++) {
        Table joinTable = (Table) joins.get(i).getRightItem();

        root = new JoinOperator(root, selectHelper(joinTable, null), null);
      }

      return root;
    }

    ExpressionSplitter e = new ExpressionSplitter();
    where.accept(e);

    Operator root = selectHelper(original, e.getConditions(original));

    for (int i = 0; i < joins.size(); i++) {
      Table joinTable = (Table) joins.get(i).getRightItem();

      root = new JoinOperator(root, selectHelper(joinTable, e.getConditions(joinTable)), where);
    }

    return root;
  }

  /**
   * A helper function that creates a DuplicateEliminationOperator if the
   * statement calls for distinct functions
   * 
   * @param child    An operator whose duplicate values are to be eliminated
   * @param distinct Iff this value is not null, duplicate values in the table
   *                 will be eliminated
   * @return A DuplicateEliminationOperator that returns distinct values from the
   *         table
   */
  private Operator distinctHelper(Operator child, Distinct distinct) {
    if (distinct != null) {
      return new DuplicateEliminationOperator(child);
    } else
      return child;
  }
}
