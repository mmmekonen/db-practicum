package common;

import java.util.ArrayList;
import java.util.Stack;
import net.sf.jsqlparser.expression.AllValue;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.ConnectByRootOperator;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonAggregateFunction;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.JsonFunction;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.OracleNamedFunctionParameter;
import net.sf.jsqlparser.expression.OverlapsCondition;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.RowGetExpression;
import net.sf.jsqlparser.expression.SafeCastExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.TimezoneExpression;
import net.sf.jsqlparser.expression.TryCastExpression;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.VariableAssignment;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.XMLSerializeExpr;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.FullTextSearch;
import net.sf.jsqlparser.expression.operators.relational.GeometryDistance;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SubSelect;

/** TODO: */
public class SelectExpressionVisitor implements ExpressionVisitor {

  private boolean condition;
  private Tuple tuple;
  private Stack<Long> stack;
  private ArrayList<Column> columns;

  /** TODO: */
  public SelectExpressionVisitor(Tuple tuple, ArrayList<Column> schema) {
    this.condition = true;
    this.tuple = tuple;
    this.stack = new Stack<>();
    this.columns = schema;
  }

  /** TODO: */
  public boolean conditionSatisfied() {
    return condition;
  }

  @Override
  public void visit(AndExpression andExpression) {
    // TODO Auto-generated method stub
    andExpression.getLeftExpression().accept(this);
    boolean left = condition;

    andExpression.getRightExpression().accept(this);
    boolean right = condition;

    condition = left && right;
  }

  @Override
  public void visit(Column tableColumn) {
    // TODO Auto-generated method stub
    String[] columnInfo = tableColumn.getColumnName().split("\\.");
    String tableName = columnInfo[0];
    String columnName = columnInfo[1];
    for (int i = 0; i < columns.size(); i++) {
      String[] columnInfo2 = columns.get(i).getColumnName().split("\\.");
      String tableName2 = columnInfo2[0];
      String columnName2 = columnInfo2[1];
      if (tableName == tableName2 && columnName == columnName2) {
        stack.push((long) tuple.getElementAtIndex(i));
        return;
      }
    }
  }

  @Override
  public void visit(LongValue longValue) {
    // TODO Auto-generated method stub
    stack.push(longValue.getValue());
  }

  @Override
  public void visit(EqualsTo equalsTo) {
    // TODO Auto-generated method stub
    equalsTo.getLeftExpression().accept(this);
    equalsTo.getRightExpression().accept(this);

    long right = stack.pop();
    long left = stack.pop();

    condition = left == right;
  }

  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    // TODO Auto-generated method stub
    notEqualsTo.getLeftExpression().accept(this);
    notEqualsTo.getRightExpression().accept(this);

    long right = stack.pop();
    long left = stack.pop();

    condition = left != right;
  }

  @Override
  public void visit(GreaterThan greaterThan) {
    // TODO Auto-generated method stub
    greaterThan.getLeftExpression().accept(this);
    greaterThan.getRightExpression().accept(this);

    long right = stack.pop();
    long left = stack.pop();

    condition = left > right;
  }

  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    // TODO Auto-generated method stub
    greaterThanEquals.getLeftExpression().accept(this);
    ;
    greaterThanEquals.getRightExpression().accept(this);

    long right = stack.pop();
    long left = stack.pop();

    condition = left >= right;
  }

  @Override
  public void visit(MinorThan minorThan) {
    // TODO Auto-generated method stub
    minorThan.getLeftExpression().accept(this);
    minorThan.getRightExpression().accept(this);
    long right = stack.pop();
    long left = stack.pop();

    condition = left < right;
  }

  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    // TODO Auto-generated method stub
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
  public void visit(UserVariable var) {
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
