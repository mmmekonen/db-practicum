package common;

import java.util.HashSet;
import java.util.Set;


public class UFElement {
    
    private HashSet<String> attributes;
    Integer upperBound;
    Integer lowerBound;

    public UFElement() {
        attributes = new HashSet<>();
    }
    
    public Set<String> getAttributes() {
        return attributes;
    }

    public Integer getLowerBound() {
        return lowerBound;
    }

    public Integer getUpperBound() {
        return upperBound;
    }

    public Integer getEqualityCon() {
        if (upperBound.equals(lowerBound)) return upperBound;
        else return null;
    }

    public void addAttribute(String attr, Integer upperBound, Integer lowerBound, Integer equalCon) {
        attributes.add(attr);
        if (upperBound < this.upperBound) this.upperBound = upperBound;
        if (lowerBound > this.lowerBound) this.lowerBound = lowerBound;
        if (equalCon != null) {
            this.upperBound = equalCon;
            this.lowerBound = equalCon;
        }
    }


}
