package logical_operator;

import java.util.List;

import common.PhysicalPlanBuilder;
import net.sf.jsqlparser.schema.Column;

/**
 * A class to represent a sort operator on a relation. This is a logical sort
 * operator taht represents a physical sort operator. The physical sort operator
 * is either an in-memory sort or an external sort.
 */
public class Sort extends LogicalOperator {

    // the operator's child oeprator
    private LogicalOperator child;

    // the list of columns to order by
    private List<Column> orderbyElements;

    /**
     * Creates a logical sort operator using a LogicalOperator and a List of Columns
     * to order by.
     *
     * @param child           The scan operator's logical child operator.
     * @param orderbyElements ArrayList of columns to order the tuples from the
     *                        child by.
     */
    public Sort(LogicalOperator child, List<Column> orderbyElements) {
        this.child = child;
        this.orderbyElements = orderbyElements;
    }

    @Override
    public void accept(PhysicalPlanBuilder builder) {
        builder.visit(this);
    }

    /**
     * Returns the logical operator of this operator's child.
     * 
     * @return the logical child operator.
     */
    public LogicalOperator getChild() {
        return child;
    }

    /**
     * Returns a the order by elements for the sort operator.
     * 
     * @return the list of columns to order by.
     */
    public List<Column> getOrderByElements() {
        return orderbyElements;
    }
}
