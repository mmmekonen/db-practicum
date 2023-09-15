package operator;

import common.SelectExpressionVisitor;
import common.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class JoinOperator extends Operator {

    private Operator left;
    private Operator right;
    private Expression expression;
    private Tuple leftTuple;
    private Tuple rightTuple;


    /**Joins two operators together according to an expression, producing a single operator that returns the next valid
     * combination of its children's tuples
     * @param left_op One of the operators to be joined
     * @param right_op One of the operators to be joine
     * @param expression An expression that dictates which combinations of tuples from the children operators are valid
     * @return a JoinOperator object
     * */
    public JoinOperator(Operator left_op, Operator right_op, Expression expression)
    {
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
    //Resets both child operators, which is equivalent to resetting the whole thing
    public void reset() {
        left.reset();
        right.reset();
    }

    /**
     * Iterates through combinations of outputs from the children operators until it finds one that is valid, then
     * returns it as a combined tuple
     * @return The next valid tuple created as a join of the two children operators
     */
    public Tuple getNextTuple() {

        if(leftTuple == null || rightTuple == null) return null;
        Tuple tuple;
        boolean satisfied = false;

        while(!satisfied && leftTuple != null) {
            ArrayList combined = leftTuple.getAllElements();
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
     * Helper function that increments the children operators over all possible combinations
     */
    private void advance() {
        rightTuple = right.getNextTuple();
        if(rightTuple == null) {
            right.reset();
            rightTuple = right.getNextTuple();
            leftTuple = left.getNextTuple();
        }
    }

}
