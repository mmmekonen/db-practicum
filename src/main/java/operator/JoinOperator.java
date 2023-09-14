package operator;

import common.DBCatalog;
import common.SelectExpressionVisitor;
import common.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class JoinOperator extends Operator {

    private Operator left;
    private Operator right;
    private Expression expression;

    /** TODO: */
    public JoinOperator(String tablename, Operator left, Operator right, Expression expression) {
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

        Tuple tuple;

        if(rightTuple != null && leftTuple != null) {
            ArrayList combined = leftTuple.getAllElements();
            combined.addAll(rightTuple.getAllElements());
            tuple = new Tuple(combined);
        } else if (leftTuple != null) {
            tuple = leftTuple;
        } else {
            tuple = rightTuple;
        }


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
            if(expression != null) {
                expression.accept(visitor);
                if (visitor.conditionSatisfied()) return tuple;
            } else return tuple;

        }

        return null;
    }


}
