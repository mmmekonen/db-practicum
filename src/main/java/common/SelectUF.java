package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import visitors.ComparisonExtractor;

/**
 * A union-find object to determine bounds on the tables in a statement, for use in pushing bounds
 * across joins
 */
public class SelectUF {

  // The elements of the union-find data structure
  private HashMap<String, UFElement> elements;

  /**
   * A constructor to create a union-find data structure from an expression
   *
   * @param e An expression which will be read into the union-find
   */
  public SelectUF(Expression e) {
    elements = new HashMap<>();
    ComparisonExtractor c = new ComparisonExtractor(this);
    e.accept(c);
  }

  /**
   * Adds an expression, and all the bounds therein, to the union-find
   *
   * @param e the expression to be added to the union-find
   */
  public void add(UFElement e) {
    ArrayList<String> temp = new ArrayList<>(e.attributes);
    for (String s : temp) {
      if (elements.containsKey(s)) {
        elements.get(s).union(e);
        e = elements.get(s);
      } else {
        elements.put(s, e);
      }
    }
  }

  /**
   * Searches for an element within the union-find
   *
   * @param attr A column used to search the data structure
   * @return A union-find element, containing a list of attributes and bounds
   */
  public UFElement find(Column attr) {
    return elements.get(attr.toString());
  }

  /**
   * A method to merge two elements of the union-find data structure
   *
   * @param att1 An attribute of one of the elements to be merged
   * @param att2 An attribute of another element to be merged
   */
  public void union(String att1, String att2) {
    if (elements.get(att1) == elements.get(att2)) return;
    UFElement result = elements.get(att1);
    ArrayList<String> attrList2 = new ArrayList<>(elements.get(att2).attributes);

    result.union(elements.get(att2));

    for (int i = 0; i < attrList2.size(); i++) {
      elements.put(attrList2.get(i), result);
    }
  }

  /**
   * A method to get the expressions not captured by the elements of the union-find
   *
   * @return An expression derived from those added to the union-find data structure
   */
  public Expression getRemainder() {
    HashSet<Expression> allRemainders = new HashSet<>();
    for (UFElement e : elements.values()) {
      allRemainders.addAll(e.getRemainder());
    }
    if (!allRemainders.isEmpty()) {
      ArrayList<Expression> remainderList = new ArrayList<>(allRemainders);
      Expression e = remainderList.remove(0);
      while (!remainderList.isEmpty()) {
        e = new AndExpression().withLeftExpression(e).withRightExpression(remainderList.remove(0));
      }
      return e;
    }
    return null;
  }

  /**
   * A usable expression that can be used on the table provided to the method
   *
   * @param table A table from which to build the predicate
   * @return An expression that can be used as the "where" clause in an SQL query
   */
  public Expression getWhere(Table table) {
    ArrayList<Column> cols = DBCatalog.getInstance().getTableSchema(table.getName());
    HashSet<Expression> conditions = new HashSet<>();
    for (Column c : cols) {
      c.setTable(table);
      if (this.find(c) != null) {
        UFElement element = find(c);
        if (element.getRemainder() != null) conditions.addAll(element.getRemainder());
        if (element.lowerBound != null && element.upperBound != null) {
          if (find(c).lowerBound != Long.MIN_VALUE) {
            GreaterThanEquals lower =
                new GreaterThanEquals()
                    .withLeftExpression(c)
                    .withRightExpression(new LongValue(find(c).lowerBound));
            conditions.add(lower);
          }
          if (find(c).upperBound != Long.MAX_VALUE) {
            MinorThanEquals upper =
                new MinorThanEquals()
                    .withLeftExpression(c)
                    .withRightExpression(new LongValue(find(c).upperBound));
            conditions.add(upper);
          }
        } else if (element.lowerBound != null && find(c).lowerBound != Long.MIN_VALUE) {
          conditions.add(
              new GreaterThanEquals()
                  .withLeftExpression(c)
                  .withRightExpression(new LongValue(find(c).lowerBound)));
        } else if (element.upperBound != null && find(c).upperBound != Long.MAX_VALUE) {
          conditions.add(
              new MinorThanEquals()
                  .withLeftExpression(c)
                  .withRightExpression(new LongValue(find(c).upperBound)));
        }
      }
    }
    if (!conditions.isEmpty()) {
      ArrayList<Expression> conditionsList = new ArrayList<>(conditions);
      Expression e = conditionsList.remove(0);
      while (!conditionsList.isEmpty()) {
        e = new AndExpression().withLeftExpression(e).withRightExpression(conditionsList.remove(0));
      }
      return e;
    }
    return null;
  }

  /** A method to return the string representation of the union-find data structure */
  public String toString() {
    StringBuilder result = new StringBuilder();
    boolean first = true;
    for (UFElement e : new HashSet<>(elements.values())) {
      if (!first) {
        result.append("\n");
      } else {
        first = false;
      }
      result.append(e);
    }
    return result.toString();
  }
}
