package operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import common.SortComparator;
import common.Tuple;
import net.sf.jsqlparser.schema.Column;

public class SortOperator extends Operator {

    private List<Tuple> tuples;
    private int index = 0;

    /** TODO: */
    public SortOperator(Operator child, List<Column> orderbyElements) {
        super(null);
        // TODO Auto-generated constructor stub
        // get all child tuples
        this.tuples = child.getAllTuples();

        // gives the index in the tuple by order preference (ie. indexOrders[i] = the
        // index in child.outputSchema that is i-th in the final order)
        HashMap<Integer, Integer> indexOrders = new HashMap<>();
        int position = 0;
        while (position < orderbyElements.size()) {
            Column column = orderbyElements.get(position);
            for (int i = 0; i < child.outputSchema.size(); i++) {
                if (child.outputSchema.get(i).getColumnName().equals(column.getColumnName())) {
                    indexOrders.put(position, i);
                    position++;
                    break;
                }
            }
        }

        for (int i = 0; i < child.outputSchema.size(); i++) {
            if (!indexOrders.containsValue(i)) {
                indexOrders.put(position, i);
                position++;
            }
        }

        // sort the tuples
        this.tuples.sort(new SortComparator(indexOrders));

        // create output schema
        ArrayList<Column> schema = new ArrayList<>();
        for (int i = 0; i < child.outputSchema.size(); i++) {
            schema.add(child.outputSchema.get(indexOrders.get(i)));
        }
        this.outputSchema = schema;
    }

    @Override
    public void reset() {
        // TODO Auto-generated method stub
        this.index = 0;
    }

    @Override
    public Tuple getNextTuple() {
        // TODO Auto-generated method stub
        while (this.tuples.size() > this.index) {
            this.index++;
            return this.tuples.get(this.index - 1);
        }
        return null;
    }

}
