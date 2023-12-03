package common;


import net.sf.jsqlparser.schema.Column;

public class Condition {

    public enum Type {
        GT,
        LT,
        GEQ,
        LEQ,
        EQ,
        NEQ
    }

    Long val;
    Column col1;
    Column col2;

    public Condition (Column col, Long val) {
        this.col1 = col;
        this.val = val;
    }

    public boolean betweenCols() {
        return col1 != null && col2 != null;
    }

}
