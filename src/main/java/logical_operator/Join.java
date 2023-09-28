package logical_operator;

import common.PhysicalPlanBuilder;
import net.sf.jsqlparser.expression.Expression;

/**
 * TODO
 */
public class Join extends LogicalOperator {

    private LogicalOperator leftChild;
    private LogicalOperator rightChild;
    private Expression expression;

    public Join(LogicalOperator left_op, LogicalOperator right_op, Expression expression) {
        this.leftChild = left_op;
        this.rightChild = right_op;
        this.expression = expression;
    }

    @Override
    public void accept(PhysicalPlanBuilder builder) {
        builder.visit(this);
    }

    public LogicalOperator getLeftChild() {
        return leftChild;
    }

    public LogicalOperator getRightChild() {
        return rightChild;
    }

    public Expression getExpression() {
        return expression;
    }
}
