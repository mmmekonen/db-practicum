package logical_operator;

import common.PhysicalPlanBuilder;

/**
 * TODO
 */
public class DuplicateElimination extends LogicalOperator {

    private LogicalOperator child;

    public DuplicateElimination(LogicalOperator child) {
        this.child = child;
    }

    @Override
    public void accept(PhysicalPlanBuilder builder) {
        builder.visit(this);
    }

    public LogicalOperator getChild() {
        return child;
    }
}
