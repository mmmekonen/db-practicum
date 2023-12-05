package visitors;

import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * A class to evaluate the WHERE clause of queries. Given a tuple and its column schema, it uses a
 * visitor pattern to evaluate the accepted Expression on the current tuple. The final result can be
 * accessed through the conditionSatisfied function.
 */
public class SelectExpressionVisitor implements ExpressionVisitor {

  // the final condition
  private boolean condition;

  // the current tuple the expression is being evaluated on
  private Tuple tuple;

  // stack contianing the current values to use in the expression
  private Stack<Long> stack;

  // the columns corresponding to each tuple position
  private ArrayList<Column> columns;

  /**
   * Creates an expression visitor using a Tuple and an ArrayList of Columns.
   *
   * @param tuple a Tuple of integers.
   * @param schema ArrayList of columns which correspond to the respective tuple values.
   */
  public SelectExpressionVisitor(Tuple tuple, List<Column> schema) {
    this.condition = true;
    this.tuple = tuple;
    this.stack = new Stack<>();
    this.columns = new ArrayList<>(schema);
  }

  /**
   * Returns a boolean on whether the predicate was satisfied by the tuple.
   *
   * @return True if the expression evalutes to true for the current tuple, otherwise false.
   */
  public boolean conditionSatisfied() {
    return condition;
  }

  /**
   * Visits and evaluates each part of the expression
   *
   * @param andExpression The expression to be visited
   */
  @Override
  public void visit(AndExpression andExpression) {
    boolean left;
    boolean right;
    try {
      andExpression.getLeftExpression().accept(this);
      left = condition;
    } catch (IndexOutOfBoundsException e) {
      left = true;
    }

    try {
      andExpression.getRightExpression().accept(this);
      right = condition;
    } catch (IndexOutOfBoundsException e) {
      right = true;
    }

    condition = left && right;
  }

  /**
   * Visits a column and selects relevant elements
   *
   * @param tableColumn The column to be visited
   */
  @Override
  public void visit(Column tableColumn) {
    String name = tableColumn.getColumnName();
    String tableName = tableColumn.getTable().getName();
    tableName =
        tableColumn.getTable().getAlias() != null
            ? tableColumn.getTable().getAlias().getName()
            : tableName;
    for (int i = 0; i < columns.size(); i++) {
      String name2 = columns.get(i).getColumnName();

      Table t = columns.get(i).getTable();
      String tableName2 = t.getAlias() != null ? t.getAlias().getName() : t.getName();
      // check if the column in the tuple is the same as the one in the expression
      if (name.equals(name2) && tableName.equals(tableName2)) {
        stack.push((long) tuple.getElementAtIndex(i));
        return;
      }
    }
  }

  /**
   * Visits a LongValue and pushes it onto the stack
   *
   * @param longValue The LongValue to be visited
   */
  @Override
  public void visit(LongValue longValue) {
    stack.push(longValue.getValue());
  }

  /**
   * Visits and evaluates all parts of the expression
   *
   * @param equalsTo The expression to be visited
   */
  @Override
  public void visit(EqualsTo equalsTo) {
    equalsTo.getLeftExpression().accept(this);
    equalsTo.getRightExpression().accept(this);

    long right = stack.pop();
    long left = stack.pop();

    condition = left == right;
  }

  /**
   * Visits and evaluates all parts of the expression
   *
   * @param notEqualsTo The expression to be visited
   */
  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    notEqualsTo.getLeftExpression().accept(this);
    notEqualsTo.getRightExpression().accept(this);

    long right = stack.pop();
    long left = stack.pop();

    condition = left != right;
  }

  /**
   * Visits and evaluates all parts of the expression
   *
   * @param greaterThan The expression to be visited
   */
  @Override
  public void visit(GreaterThan greaterThan) {
    greaterThan.getLeftExpression().accept(this);
    greaterThan.getRightExpression().accept(this);

    long right = stack.pop();
    long left = stack.pop();

    condition = left > right;
  }

  /**
   * Visits and evaluates all parts of the expression
   *
   * @param greaterThanEquals The expression to be visited
   */
  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    greaterThanEquals.getLeftExpression().accept(this);
    greaterThanEquals.getRightExpression().accept(this);

    long right = stack.pop();
    long left = stack.pop();

    condition = left >= right;
  }

  /**
   * Visits and evaluates all parts of the expression
   *
   * @param minorThan The expression to be visited
   */
  @Override
  public void visit(MinorThan minorThan) {
    minorThan.getLeftExpression().accept(this);
    minorThan.getRightExpression().accept(this);
    long right = stack.pop();
    long left = stack.pop();

    condition = left < right;
  }

  /**
   * Visits and evaluates all parts of the expression
   *
   * @param minorThanEquals The expression to be visited
   */
  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    minorThanEquals.getLeftExpression().accept(this);
    minorThanEquals.getRightExpression().accept(this);
    long right = stack.pop();
    long left = stack.pop();

    condition = left <= right;
  }

  // unused operators
  @Override
  public void visit(BitwiseRightShift aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(BitwiseLeftShift aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(NullValue nullValue) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Function function) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(SignedExpression signedExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(JdbcParameter jdbcParameter) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(JdbcNamedParameter jdbcNamedParameter) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(DoubleValue doubleValue) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(HexValue hexValue) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(DateValue dateValue) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(TimeValue timeValue) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(TimestampValue timestampValue) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Parenthesis parenthesis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(StringValue stringValue) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Addition addition) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Division division) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(IntegerDivision division) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Multiplication multiplication) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Subtraction subtraction) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(OrExpression orExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(XorExpression orExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Between between) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(OverlapsCondition overlapsCondition) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(InExpression inExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(FullTextSearch fullTextSearch) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(IsNullExpression isNullExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(IsBooleanExpression isBooleanExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(LikeExpression likeExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(SubSelect subSelect) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(CaseExpression caseExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(WhenClause whenClause) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(ExistsExpression existsExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(AnyComparisonExpression anyComparisonExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Concat concat) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Matches matches) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(BitwiseAnd bitwiseAnd) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(BitwiseOr bitwiseOr) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(BitwiseXor bitwiseXor) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(CastExpression cast) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(TryCastExpression cast) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(SafeCastExpression cast) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(Modulo modulo) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(AnalyticExpression aexpr) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(ExtractExpression eexpr) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(IntervalExpression iexpr) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(OracleHierarchicalExpression oexpr) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(RegExpMatchOperator rexpr) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(JsonExpression jsonExpr) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(JsonOperator jsonExpr) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(RegExpMySQLOperator regExpMySQLOperator) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(UserVariable uvar) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(NumericBind bind) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(KeepExpression aexpr) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(MySQLGroupConcat groupConcat) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(ValueListExpression valueList) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(RowConstructor rowConstructor) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(RowGetExpression rowGetExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(OracleHint hint) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(TimeKeyExpression timeKeyExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(DateTimeLiteralExpression literal) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(NotExpression aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(NextValExpression aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(CollateExpression aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(SimilarToExpression aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(ArrayExpression aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(ArrayConstructor aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(VariableAssignment aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(XMLSerializeExpr aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(TimezoneExpression aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(JsonAggregateFunction aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(JsonFunction aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(ConnectByRootOperator aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(OracleNamedFunctionParameter aThis) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(AllColumns allColumns) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(AllTableColumns allTableColumns) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(AllValue allValue) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(IsDistinctExpression isDistinctExpression) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }

  @Override
  public void visit(GeometryDistance geometryDistance) {
    throw new UnsupportedOperationException("Unimplemented method 'visit'");
  }
}
