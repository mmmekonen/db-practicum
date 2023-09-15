package common;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.*;

public class ExpressionSorter implements ExpressionVisitor {

    private boolean isSingle;
    private int depth;
    private HashSet<Table> tables;
    private Table latestTable;

    public ExpressionSorter() {
        this.isSingle = true;
        this.depth = 0;
        this.tables = new HashSet<Table>();
        this.latestTable = null;

    }

    /** TODO: */
    public boolean onSingleTable() {
        return isSingle;
    }

    public int depth(Expression e) {
        return depth;
    }

    public void checkTable(Table t) {
        tables.add(t);
        isSingle = tables.size() == 1;
    }

    public Table getTable() {
        if (isSingle)
            return latestTable;
        else
            return null;
    }

    public Set<Table> getTables() {
        return tables;
    }

    @Override
    public void visit(AndExpression andExpression) {
        // TODO Auto-generated method stub
        andExpression.getLeftExpression().accept(this);

        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(Column tableColumn) {
        // TODO Auto-generated method stub
        checkTable(tableColumn.getTable());
        latestTable = tableColumn.getTable();
    }

    @Override
    public void visit(LongValue longValue) {
        // :)
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        // TODO Auto-generated method stub
        equalsTo.getLeftExpression().accept(this);
        equalsTo.getRightExpression().accept(this);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        // TODO Auto-generated method stub
        notEqualsTo.getLeftExpression().accept(this);
        notEqualsTo.getRightExpression().accept(this);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        // TODO Auto-generated method stub
        greaterThan.getLeftExpression().accept(this);
        greaterThan.getRightExpression().accept(this);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        // TODO Auto-generated method stub
        greaterThanEquals.getLeftExpression().accept(this);
        greaterThanEquals.getRightExpression().accept(this);

    }

    @Override
    public void visit(MinorThan minorThan) {
        // TODO Auto-generated method stub
        minorThan.getLeftExpression().accept(this);
        minorThan.getRightExpression().accept(this);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        // TODO Auto-generated method stub
        minorThanEquals.getLeftExpression().accept(this);
        minorThanEquals.getRightExpression().accept(this);
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
