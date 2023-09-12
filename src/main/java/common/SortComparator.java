package common;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Comparator;

import net.sf.jsqlparser.schema.Column;
import operator.Operator;

public class SortComparator implements Comparator<Tuple> {

    private ArrayList<Integer> sortOrder;

    /** TODO: */
    public SortComparator(ArrayList<Integer> sortOrder) {
        super();
        // TODO Auto-generated constructor stub
        this.sortOrder = sortOrder;
    }

    @Override
    public int compare(Tuple o1, Tuple o2) {
        // TODO Auto-generated method stub

        for (int i = 0; i < this.sortOrder.size(); i++) {
            int comp = Integer.compare(o1.getElementAtIndex(this.sortOrder.get(i)),
                    o2.getElementAtIndex(this.sortOrder.get(i)));
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }

}
