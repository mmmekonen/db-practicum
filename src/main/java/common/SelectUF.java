package common;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import visitors.ComparisonExtractor;

public class SelectUF {

    private HashMap<String, UFElement> elements;

    public SelectUF(Expression e) {
        elements = new HashMap<>();
        ComparisonExtractor c = new ComparisonExtractor(this);
        e.accept(c);
        
    }

    public void add(UFElement e) {
        ArrayList<String> temp = new ArrayList<>(e.getAttributes());
        for (String s : temp) {
            if (elements.containsKey(s)) {
                elements.get(s).union(e);
                e = elements.get(s);
            } else {
                elements.put(s, e);
            }
        }
        

    }

    public UFElement find(Column attr) {
        return elements.get(attr.toString());
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

    public Expression getWhere(Table table) {
        ArrayList<Column> cols = DBCatalog.getInstance().getTableSchema(table.getDBLinkName());
        ArrayList<Expression> conditions = new ArrayList<>();
        for (Column c : cols) {
            if (this.find(c) != null) {
                GreaterThanEquals lower = new GreaterThanEquals()
                .withLeftExpression(c).withRightExpression(new LongValue(find(c).lowerBound));
                MinorThanEquals upper = new MinorThanEquals()
                .withLeftExpression(c).withRightExpression(new LongValue(find(c).upperBound));
                conditions.add(new AndExpression().withLeftExpression(lower).withRightExpression(upper));
            }
        }
        if (!conditions.isEmpty()) {
            Expression e = conditions.remove(0);
            while (!conditions.isEmpty()) {
                e = new AndExpression().withLeftExpression(e).withRightExpression(conditions.remove(0));
            }
            return e;
        }
        return null;
    }

}
