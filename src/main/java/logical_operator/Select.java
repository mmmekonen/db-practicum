package logical_operator;

import common.PhysicalPlanBuilder;
import net.sf.jsqlparser.expression.Expression;

/**
 * TODO
 */
public class Select extends LogicalOperator {

    private LogicalOperator child;
    private Expression expression;

    public Select(LogicalOperator child, Expression expression) {
        this.child = child;
        this.expression = expression;
    }

    public LogicalOperator getChild() {
        return child;
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public void accept(PhysicalPlanBuilder builder) {
        builder.visit(this);
    }
}
