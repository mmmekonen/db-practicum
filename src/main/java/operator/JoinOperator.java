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

    /**
     * Creates a JoinOperator object that concatenates two other operators together
     * @param left_op One operator to be joined
     * @param right_op Another operator to be joined
     * @param expression An expression that dictates what combinations of tuples are valid
     */
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

    /** Resets cursor on the operator to the beginning */
    public void reset() {
        left.reset();
        right.reset();
    }

    /**
     * Returns the next valid combination of tuples from the child operators
     * @return next Tuple, or null if we are at the end
     */
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

    /**
     * Helper function to increment the operator
     */
    private void advance() {
        rightTuple = right.getNextTuple();
        if (rightTuple == null) {
            right.reset();
            rightTuple = right.getNextTuple();
            leftTuple = left.getNextTuple();
        }
    }
}
