package common;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;

public class UFElement {

    public enum BoundType {
        UPPER,
        LOWER,
        EQUALS
    }

    private ArrayList<String> attributes;
    Long upperBound;
    Long lowerBound;

    public UFElement() {
        this.attributes = new ArrayList<>();
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public Long getEqualityCon() {
        if (upperBound == lowerBound)
            return upperBound;
        else
            return null;
    }

    public void addAttribute(Column attr, long bound, BoundType type) {
        attributes.add(attr.toString());
        if (type == BoundType.UPPER)
            this.upperBound = upperBound != null ? Math.min(bound, upperBound) : bound;
        else if (type == BoundType.LOWER)
            this.lowerBound = lowerBound != null ? Math.max(bound, lowerBound) : bound;
        else if (type == BoundType.EQUALS) {
            this.upperBound = bound;
            this.lowerBound = bound;
        }
    }

    public void addAttribute(Column attr) {
        attributes.add(attr.toString());
    }

    public void union(UFElement other) {
        this.attributes.addAll(other.attributes);
        this.upperBound = this.upperBound != null
                ? other.upperBound != null ? Math.min(other.upperBound, this.upperBound) : this.upperBound
                : other.upperBound;
        this.lowerBound = this.lowerBound != null
                ? other.lowerBound != null ? Math.max(other.lowerBound, this.lowerBound) : this.lowerBound
                : other.lowerBound;
    }

    public boolean satisfied(Tuple tuple, List<Column> schema) {
        for (int i = 0; i < schema.size(); i++) {
            if (attributes.contains(schema.get(i).toString())) {
                return tuple.getAllElements().get(i) > (lowerBound != null ? lowerBound : Long.MIN_VALUE)
                        && tuple.getAllElements().get(i) < (upperBound != null ? upperBound : Long.MAX_VALUE);
            }
        }
        return false;
    }
}
