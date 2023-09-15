package operator;

import common.DBCatalog;
import common.SelectExpressionVisitor;
import common.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;

public class JoinOperator extends Operator {

    private Operator left;
    private Operator right;
    private Expression expression;
    private Tuple leftTuple;
    private Tuple rightTuple;

    /** TODO: */
    public JoinOperator(ArrayList<Column> schema, Operator left_op, Operator right_op, Expression expression)
    {
        super(schema);
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


        if(leftTuple == null || rightTuple == null) return null;

        Tuple tuple;


        ArrayList schema = left.outputSchema;
        schema.addAll(right.outputSchema);

        boolean satisfied = false;

        while(!satisfied && leftTuple != null) {
            ArrayList combined = leftTuple.getAllElements();
            combined.addAll(rightTuple.getAllElements());
            tuple = new Tuple(combined);

            SelectExpressionVisitor visitor = new SelectExpressionVisitor(tuple, schema);
            expression.accept(visitor);
            satisfied = visitor.conditionSatisfied();
            if (satisfied) {
                advance();
                System.out.println(tuple);
                return tuple;
            }

            advance();


        }


        /*while(leftTuple != null) {

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

        }*/

        return null;
    }

    private void advance() {
        rightTuple = right.getNextTuple();
        if(rightTuple == null) {
            right.reset();
            rightTuple = right.getNextTuple();
            leftTuple = left.getNextTuple();
        }
    }


}
