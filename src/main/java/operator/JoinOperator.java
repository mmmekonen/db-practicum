package operator;

import java.util.ArrayList;

import common.SelectExpressionVisitor;
import common.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class JoinOperator extends Operator {

    private Operator left;
    private Operator right;
    private Expression expression;
    private Tuple leftTuple;
    private Tuple rightTuple;

    /** TODO: */
    public JoinOperator(Operator left_op, Operator right_op, Expression expression) {
        super(null);
        this.outputSchema = left_op.getOutputSchema();
        this.outputSchema.addAll(right_op.getOutputSchema());
        this.left = left_op;
        this.right = right_op;
        this.expression = expression;
        this.leftTuple = left.getNextTuple();
        this.rightTuple = right.getNextTuple();
    }

    public void reset() {
        left.reset();
        right.reset();
    }

    public Tuple getNextTuple() {

        if (leftTuple == null || rightTuple == null)
            return null;

        Tuple tuple;

        boolean satisfied = false;

        while (!satisfied && leftTuple != null) {
            ArrayList<Integer> combined = leftTuple.getAllElements();
            combined.addAll(rightTuple.getAllElements());
            tuple = new Tuple(combined);

            SelectExpressionVisitor visitor = new SelectExpressionVisitor(tuple, outputSchema);
            expression.accept(visitor);
            satisfied = visitor.conditionSatisfied();
            if (satisfied) {
                advance();
                return tuple;
            }

            advance();
        }

        return null;
    }

    private void advance() {
        rightTuple = right.getNextTuple();
        if (rightTuple == null) {
            right.reset();
            rightTuple = right.getNextTuple();
            leftTuple = left.getNextTuple();
        }
    }
}
