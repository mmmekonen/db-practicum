package logical_operator;

import common.PhysicalPlanBuilder;
import net.sf.jsqlparser.expression.Alias;

/**
 * TODO
 */
public class Scan extends LogicalOperator {

    private String tableName;
    private Alias alias;

    public Scan(String tableName, Alias alias) {
        this.tableName = tableName;
        this.alias = alias;
    }

    @Override
    public void accept(PhysicalPlanBuilder builder) {
        builder.visit(this);
    }

    public String getTableName() {
        return tableName;
    }

    public Alias getAlias() {
        return alias;
    }
}
