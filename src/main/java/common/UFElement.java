package common;


import java.util.ArrayList;
import java.util.List;


public class UFElement implements Cloneable {

    enum BoundType {
        UPPER,
        LOWER,
        EQUALS
    }
    
    private ArrayList<String> attributes;
    long upperBound;
    long lowerBound;

    public UFElement(String attr) {
        this.attributes = new ArrayList<>();
        attributes.add(attr);
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

    public void addAttribute(String attr, long bound, BoundType type) {
        attributes.add(attr);
        if (type == BoundType.UPPER) this.upperBound = Math.min(bound, upperBound);
        else if (type == BoundType.LOWER) this.lowerBound = Math.max(bound, lowerBound);
        else if (type == BoundType.EQUALS) {
            this.upperBound = bound;
            this.lowerBound = bound;
        }
    }

    public void union(UFElement other) {
        this.attributes.addAll(other.attributes);
        this.upperBound = Math.min(other.upperBound, this.upperBound);
        this.lowerBound = Math.max(other.lowerBound, this.lowerBound);
    }

}
