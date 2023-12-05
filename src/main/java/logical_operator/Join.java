package logical_operator;

import common.*;
import java.util.*;
import net.sf.jsqlparser.expression.Expression;

/**
 * A class to represent a join. This is the logical representation of the
 * physical join operator.
 * The physical join operator can either be a tuple-nested loop join, a
 * block-nested loop join, or a
 * sort-merge join.
 */
public class Join extends LogicalOperator {

  private ArrayList<LogicalOperator> children;

  // the expression to check if the tuples should be joined
  private Expression expression;

  // the union-find object corresponding to the join
  private SelectUF uf;

  /**
   * Creates a logical join operator that concatenates two other operators
   * together using a
   * tuple-nested loop join.
   *
   * @param children   One logical operator to be joined.
   * @param expression An expression that dictates what combinations of tuples are
   *                   valid.
   */
  public Join(Collection<LogicalOperator> children, Expression expression, SelectUF uf) {
    this.children = new ArrayList<>(children);
    this.expression = expression;
    this.uf = uf;
  }

  @Override
  public void accept(PlanBuilder builder) {
    builder.visit(this);
  }

  public List<LogicalOperator> getChildren() {
    return children;
  }

  /**
   * Returns the expression of this operator.
   *
   * @return a JSQLParser Expression.
   */
  public Expression getExpression() {
    return expression;
  }

  /**
   * Returns the union-find of this operator.
   *
   * @return a SelectUF object.
   */
  public SelectUF getUnionFind() {
    return uf;
  }

  /**
   * @return A string representation of the operator (does not include the
   *         union-find elements)
   */
  public String toString() {
    // not sure if this is correct
    Expression r = uf.getRemainder();
    return "Join" + (r != null ? "[" + r + "]" : "") + "\n" + uf;
  }
}
