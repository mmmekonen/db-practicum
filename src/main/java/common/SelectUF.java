package common;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import visitors.ComparisonExtractor;

public class SelectUF {

    private HashMap<String, UFElement> elements;

    public SelectUF(Expression e) {
        elements = new HashMap<>();
        ComparisonExtractor c = new ComparisonExtractor(this);
        e.accept(c);
        
    }

    public boolean contains(String attr) {
        return elements.containsKey(attr);
    }

    public void add(UFElement e) {
        ArrayList<String> temp = new ArrayList<>(e.getAttributes());
        for (String s : temp) {
                this.find(s).union(e);
                e = this.find(s);
            } else {
                elements.put(s, e);
            }
        }
        

    }

    public UFElement find(String att) {
        return elements.get(att);
    }

    public void union(String att1, String att2) {
        if (elements.get(att1) == elements.get(att2)) return;
        UFElement result = elements.get(att1);
        ArrayList<String> attrList2 = new ArrayList<>(elements.get(att2).getAttributes());

        result.union(elements.get(att2));
        
        for(int i = 0; i < attrList2.size(); i++) {
            elements.put(attrList2.get(i), result);
        }
    }

}
