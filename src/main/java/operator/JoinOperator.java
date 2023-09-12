package operator;

import common.DBCatalog;
import common.SelectExpressionVisitor;
import common.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class JoinOperator extends Operator {

    private ScanOperator left;
    private ScanOperator right;
    private Expression expression;

    /** TODO: */
    public JoinOperator(String tablename, ScanOperator left, ScanOperator right, Expression expression) {
        super(DBCatalog.getInstance().getTableSchema(tablename));
        this.left = left;
        this.right = right;
        this.expression = expression;
    }

    public void reset() {
        left.reset();
        right.reset();
    }

    public Tuple getNextTuple() {

        Tuple leftTuple = left.getNextTuple();
        Tuple rightTuple = right.getNextTuple();

        ArrayList combined = leftTuple.getAllElements();
        combined.addAll(rightTuple.getAllElements());
        Tuple tuple = new Tuple(combined);

        ArrayList schema = left.outputSchema;
        schema.addAll(right.outputSchema);

        while(leftTuple != null) {

            if(rightTuple == null) {
                leftTuple = left.getNextTuple();
                right.reset();
                rightTuple = right.getNextTuple();
            } else {
                rightTuple = right.getNextTuple();
            }

            SelectExpressionVisitor visitor = new SelectExpressionVisitor(tuple, schema);
            expression.accept(visitor);
            if (visitor.conditionSatisfied()) return tuple;
        }

        return null;
    }


}
