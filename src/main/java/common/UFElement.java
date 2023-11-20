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
    long upperBound;
    long lowerBound;

    public UFElement() {
        this.attributes = new ArrayList<>();
        this.upperBound = Long.MAX_VALUE;
        this.lowerBound = Long.MIN_VALUE;
    }
    
    public List<String> getAttributes() {
        return attributes;
    }

    public Long getEqualityCon() {
        if (upperBound == lowerBound) return upperBound;
        else return null;
    }

    public void addAttribute(Column attr, long bound, BoundType type) {
        attributes.add(attr.toString());
        if (type == BoundType.UPPER) this.upperBound = Math.min(bound, upperBound);
        else if (type == BoundType.LOWER) this.lowerBound = Math.max(bound, lowerBound);
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
        this.upperBound = Math.min(other.upperBound, this.upperBound);
        this.lowerBound = Math.max(other.lowerBound, this.lowerBound);
    }

    public boolean satisfied(Tuple tuple, List<Column> schema) {
        for (int i = 0; i < schema.size(); i++) {
            if (attributes.contains(schema.get(i).toString())) {
                return tuple.getAllElements().get(i) > lowerBound && tuple.getAllElements().get(i) < upperBound;
            }
        }
        return false;
    }


}
