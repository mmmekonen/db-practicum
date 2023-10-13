package physical_operator;

import common.SelectExpressionVisitor;
import common.Tuple;
import java.util.ArrayList;
import net.sf.jsqlparser.expression.Expression;

/**
 * A class to represent a block-nested loop join. It loads blocks of tuples into the buffer and iterates over them,
 * instead of making an I/O request for each individual tuple.
 */
public class BNLJOperator extends Operator {

    private Operator left;
    private Operator right;
    private Expression expression;
    private Tuple leftTuple;
    private Tuple rightTuple;
    private Tuple[] buffer;
    private int pointer;

    /**
     * Creates a BNLJOperator object that concatenates two other operators together using a
     * tuple-nested loop join.
     *
     * @param left_op One operator to be joined.
     * @param right_op Another operator to be joined.
     * @param expression An expression that dictates what combinations of tuples are valid.
     */
    public BNLJOperator(Operator left_op, Operator right_op, Expression expression, int bufferPages) {
        super(null);
        this.outputSchema = left_op.getOutputSchema();
        this.outputSchema.addAll(right_op.getOutputSchema());
        this.left = left_op;
        this.right = right_op;
        this.expression = expression;
        this.leftTuple = left.getNextTuple();
        this.rightTuple = right.getNextTuple();
        this.buffer = new Tuple[bufferPages * (4096 - 8) / (4 * leftTuple.size())];
        this.pointer = buffer.length;
    }

    /**
     * Populates the buffer array with tuples from the outer table
     */
    private void fillBuffer() {
        buffer = new Tuple[buffer.length];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = leftTuple;
            leftTuple = left.getNextTuple();
        }
    }

    /** Resets cursor on the operator to the beginning */
    public void reset() {
        left.reset();
        right.reset();
    }

    /**
     * Returns the next valid combination of tuples from the child operators
     *
     * @return next Tuple, or null if we are at the end
     */
    public Tuple getNextTuple() {

        if (buffer[pointer] == null || rightTuple == null) return null;

        Tuple tuple;

        boolean satisfied = false;

        while (!satisfied && buffer[pointer] != null) {
            ArrayList<Integer> combined = buffer[pointer].getAllElements();
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

    /** Helper function to increment the operator */
    private void advance() {
        rightTuple = right.getNextTuple();
        if (rightTuple == null) {
            right.reset();
            rightTuple = right.getNextTuple();
            pointer++;
            if (pointer >= buffer.length) {
                pointer = 0;
                fillBuffer();
            }
        }
    }
}
