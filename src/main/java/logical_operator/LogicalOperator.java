package logical_operator;

import common.PhysicalPlanBuilder;

/**
 * Abstract class to represent logical operators. Every operator has a
 * reference to an outputSchema which represents the schema of the output tuples
 * from the operator. This is a list of Column objects. Each Column has an
 * embedded Table object with the name and alias (if required) fields set
 * appropriately.
 */
public abstract class LogicalOperator {

    public LogicalOperator() {
    }

    public abstract void accept(PhysicalPlanBuilder builder);
}
