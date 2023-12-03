package common;

import java.util.*;

import net.sf.jsqlparser.schema.Column;

public class Predicate {

    HashSet<Condition> conditions;

    public Predicate() {
        conditions = new HashSet<>();
    }

    public void addCondition(Condition c) {
        conditions.add(c);
    }

    public boolean satisfied(Tuple t, List<Column> schema) {
        boolean result = true;
        ArrayList temp = new ArrayList<>(conditions);
        for (int i = 0; i < schema.size(); i++) {
            for (int j = 0; i < conditions.size(); j++) {

            }
        }
        
        return result;
    }
}
