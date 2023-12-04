package common;

import java.util.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import visitors.SelectExpressionVisitor;

/**
 * An element of a union-find data structure
 */
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

  /**
   * A basic constructor for the element
   */
  public UFElement() {
    this.attributes = new ArrayList<>();
    this.remainder = new HashSet<>();
  }

  /**
   * Returns the equality condition of the element, if there is one
   * @return a Long corresponding to the equality condition of the element
   */
  public Long getEqualityCon() {
    if (upperBound != null && lowerBound != null && upperBound.equals(lowerBound))
      return upperBound;
    else return null;
  }

  /**
   * Returns the set of remainder expressions in the element
   * @return A set of binary expressions
   */
  public Set<Expression> getRemainder() {
    return remainder;
  }

  /**
   * Concatenates the remainder expressions into a single expression
   * @return A single expression corresponding to the remainders held by the element
   */
  public Expression getRemainderExpression() {
    Expression e = null;
    ArrayList<Expression> remainderList = new ArrayList<>(remainder);
    if (!remainderList.isEmpty()) {
      e = remainderList.get(0);
      for (int i = 1; i < remainderList.size(); i++) e = new AndExpression(e, remainderList.get(i));
    }
    return e;
  }

  /**
   * Adds an expression to the set of remainders held by the element
   * @param e The expression to be added
   */
  public void addRemainder(Expression e) {
    remainder.add(e);
  }

  /**
   * A method to add an attribute to the element
   * @param attr The attribute to be added
   * @param bound A bound corresponding with that attribute
   * @param type The type of bound
   */
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

  /**
   * A method to add an attribute to the element
   * @param attr The attribute to be added
   */
  public void addAttribute(Column attr) {
    attributes.add(attr.toString());
  }

  /**
   * A method to merge this element with another, preserving data across 
   * both elememnts
   * @param other The element to be merged with
   */
  public void union(UFElement other) {
    this.attributes.addAll(other.attributes);
    this.upperBound =
        this.upperBound != null
            ? (other.upperBound != null
                ? Math.min(other.upperBound, this.upperBound)
                : this.upperBound)
            : (other.upperBound != null ? other.upperBound : Long.MAX_VALUE);
    this.lowerBound =
        this.lowerBound != null
            ? (other.lowerBound != null
                ? Math.max(other.lowerBound, this.lowerBound)
                : this.lowerBound)
            : (other.lowerBound != null ? other.lowerBound : Long.MIN_VALUE);
    this.remainder.addAll(other.remainder);
  }

  /**
   * A method to determine if a tuple satisfies the bounds set by this element
   * @param tuple the tuple to be checked
   * @param schema the schema of the tuple
   * @return true iff the tuple satisfies the bounds stored in this element
   */
  public boolean satisfied(Tuple tuple, List<Column> schema) {
    SelectExpressionVisitor visitor = new SelectExpressionVisitor(tuple, schema);
    this.getRemainderExpression().accept(visitor);
    for (int i = 0; i < schema.size(); i++) {
      if (attributes.contains(schema.get(i).toString())) {
        return tuple.getAllElements().get(i) > (lowerBound != null ? lowerBound : Long.MIN_VALUE)
            && tuple.getAllElements().get(i) < (upperBound != null ? upperBound : Long.MAX_VALUE)
            && visitor.conditionSatisfied();
      }
    }
    return false;
  }


  /**
   * A method to return a string representation of this element
   */
  public String toString() {
    return "["
        + attributes
        + ", equals "
        + (getEqualityCon() != null ? getEqualityCon().toString() : "null")
        + ", min "
        + (lowerBound != null ? lowerBound.toString() : "null")
        + ", max "
        + (upperBound != null ? upperBound.toString() : "null")
        + "]";
  }
}
