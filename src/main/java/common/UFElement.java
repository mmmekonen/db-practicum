package common;

import java.util.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import visitors.SelectExpressionVisitor;

public class UFElement {

  public enum BoundType {
    UPPER,
    LOWER,
    EQUALS
  }

  ArrayList<String> attributes;
  HashSet<Expression> remainder;
  Long upperBound;
  Long lowerBound;

  public UFElement() {
    this.attributes = new ArrayList<>();
    this.remainder = new HashSet<>();
  }

  public Long getEqualityCon() {
    if (upperBound.equals(lowerBound))
      return upperBound;
    else
      return null;
  }

  public Expression getRemainder() {
    Expression e = null;
    ArrayList<Expression> remainderList = new ArrayList<>(remainder);
    if (!remainderList.isEmpty()) {
      e = remainderList.get(0);
      for (int i = 1; i < remainderList.size(); i++)
        e = new AndExpression(e, remainderList.get(i));
    }
    return e;
  }

  public void addRemainder(Expression e) {
    remainder.add(e);
  }

  public void addAttribute(Column attr, long bound, BoundType type) {
    attributes.add(attr.toString());
    if (type == BoundType.UPPER)
      this.upperBound = upperBound != null ? Math.min(bound, upperBound) : bound;
    else if (type == BoundType.LOWER)
      this.lowerBound = lowerBound != null ? Math.max(bound, lowerBound) : bound;
    else if (type == BoundType.EQUALS) {
      this.upperBound = bound;
      this.lowerBound = bound;
    }
  }

  public void addAttribute(Column attr) {
    attributes.add(attr.toString());
  }

  public void union(UFElement other) {
    this.attributes.addAll(other.attributes);
    this.upperBound = this.upperBound != null
        ? (other.upperBound != null
            ? Math.min(other.upperBound, this.upperBound)
            : this.upperBound)
        : (other.upperBound != null ? other.upperBound : Long.MAX_VALUE);
    this.lowerBound = this.lowerBound != null
        ? (other.lowerBound != null
            ? Math.max(other.lowerBound, this.lowerBound)
            : this.lowerBound)
        : (other.lowerBound != null ? other.lowerBound : Long.MIN_VALUE);
    this.remainder.addAll(other.remainder);
  }

  public boolean satisfied(Tuple tuple, List<Column> schema) {
    SelectExpressionVisitor visitor = new SelectExpressionVisitor(tuple, schema);
    this.getRemainder().accept(visitor);
    for (int i = 0; i < schema.size(); i++) {
      if (attributes.contains(schema.get(i).toString())) {
        return tuple.getAllElements().get(i) > (lowerBound != null ? lowerBound : Long.MIN_VALUE)
            && tuple.getAllElements().get(i) < (upperBound != null ? upperBound : Long.MAX_VALUE)
            && visitor.conditionSatisfied();
      }
    }
    return false;
  }

  public String toString() {
    return "["
        + attributes
        + ", equals "
        + getEqualityCon()
        + ", min "
        + lowerBound
        + ", max "
        + upperBound
        + "]";
  }
}
