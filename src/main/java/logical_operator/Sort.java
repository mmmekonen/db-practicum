package logical_operator;

import java.util.List;

import common.PhysicalPlanBuilder;
import net.sf.jsqlparser.schema.Column;

/**
 * TODO
 */
public class Sort extends LogicalOperator {

    private LogicalOperator child;
    private List<Column> orderbyElements;

    public Sort(LogicalOperator child, List<Column> orderbyElements) {
        this.child = child;
        this.orderbyElements = orderbyElements;
    }

    @Override
    public void accept(PhysicalPlanBuilder builder) {
        builder.visit(this);
    }

    public LogicalOperator getChild() {
        return child;
    }

    public List<Column> getOrderByElements() {
        return orderbyElements;
    }
}
