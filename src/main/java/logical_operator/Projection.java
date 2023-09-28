package logical_operator;

import java.util.List;

import common.PhysicalPlanBuilder;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * TODO
 */
public class Projection extends LogicalOperator {

    private List<SelectItem> selectItems;
    private LogicalOperator child;

    public Projection(List<SelectItem> selectItems, LogicalOperator child) {
        this.selectItems = selectItems;
        this.child = child;
    }

    @Override
    public void accept(PhysicalPlanBuilder builder) {
        builder.visit(this);
    }

    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    public LogicalOperator getChild() {
        return child;
    }
}
