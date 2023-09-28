package common;

import logical_operator.DuplicateElimination;
import logical_operator.Join;
import logical_operator.Projection;
import logical_operator.Scan;
import logical_operator.Select;
import logical_operator.Sort;
import physical_operator.DuplicateEliminationOperator;
import physical_operator.InMemorySortOperator;
import physical_operator.Operator;
import physical_operator.ProjectionOperator;
import physical_operator.ScanOperator;
import physical_operator.SelectOperator;
import physical_operator.TNLJOperator;

/**
 * TODO
 */
public class PhysicalPlanBuilder {

    Operator root;

    public PhysicalPlanBuilder() {
    }

    public Operator getRoot() {
        return root;
    }

    public void visit(Scan scanOp) {
        root = new ScanOperator(scanOp.getTableName(), scanOp.getAlias());
    }

    public void visit(Select selectOp) {
        selectOp.getChild().accept(this);

        Operator child = root;
        root = new SelectOperator(child, selectOp.getExpression());
    }

    public void visit(Projection projectionOp) {
        projectionOp.getChild().accept(this);

        Operator child = root;
        root = new ProjectionOperator(projectionOp.getSelectItems(), child);
    }

    public void visit(Join joinOp) {
        joinOp.getLeftChild().accept(this);
        Operator left = root;

        joinOp.getRightChild().accept(this);
        Operator right = root;

        root = new TNLJOperator(left, right, joinOp.getExpression());
    }

    public void visit(Sort sortOp) {
        sortOp.getChild().accept(this);
        Operator child = root;

        root = new InMemorySortOperator(child, sortOp.getOrderByElements());
    }

    public void visit(DuplicateElimination duplicateOp) {
        duplicateOp.getChild().accept(this);
        Operator child = root;

        root = new DuplicateEliminationOperator(child);
    }
}
