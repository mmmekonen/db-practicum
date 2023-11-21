package common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import logical_operator.Join;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import physical_operator.Operator;
import physical_operator.ScanOperator;

/** TODO */
public class DetermineJoinOrder {

    private ArrayList<Operator> allTables;
    private SelectUF uf;
    private HashMap<String, HashMap<String, Double>> reductionInfo;
    // cost of best plan
    HashMap<HashSet<Operator>, Integer> costMap;
    // expected output size of each relation
    HashMap<HashSet<Operator>, Integer> sizeMap;
    // v-values for relations
    HashMap<HashSet<Operator>, HashMap<String, Integer>> vMap;
    // actual best plan order
    HashMap<HashSet<Operator>, ArrayList<Operator>> bestPlan;

    /** TODO */
    public DetermineJoinOrder(ArrayList<Operator> ops, SelectUF unionFind,
            HashMap<String, HashMap<String, Double>> reductionInfo) {
        allTables = ops;
        uf = unionFind;
        this.reductionInfo = reductionInfo;

        // iterate through all subsets
        ArrayList<HashSet<Operator>> currSubsets = new ArrayList<>();
        for (int i = 1; i < allTables.size(); i++) {
            createSubsets(allTables, i, 0, new HashSet<>(), currSubsets);

            // loop through them
            for (HashSet<Operator> joinSet : currSubsets) {
                findJoinOrder(joinSet);
            }

            // reset for next size
            currSubsets.clear();
        }
    }

    /** TODO */
    private void findJoinOrder(HashSet<Operator> set) {
        // for size == 1, just relation, no cost
        if (set.size() == 1) {
            costMap.put(set, 0);
            bestPlan.put(set, new ArrayList<>(set));
        }

        // for size == 2, no cost
        else if (set.size() == 2) {
            costMap.put(set, 0);

            // calc size of both tables
            ArrayList<Operator> best = new ArrayList<>();
            int lowest = Integer.MAX_VALUE;
            for (Operator t : set) {
                HashSet<Operator> temp = new HashSet<>();
                temp.add(t);

                // smaller one on left
                if (sizeOfRelation(temp) < lowest)
                    best.add(0, t);
                else
                    best.add(t);
            }

            // add plan to map
            bestPlan.put(set, best);
        }

        // for size >= 3
        else {
            ArrayList<Operator> plan = new ArrayList<>();
            int lowestCost = Integer.MAX_VALUE;
            // get all possible left relations and
            // loop through them to determine cost/best left relation
            for (Operator t : set) {
                HashSet<Operator> rightRelation = new HashSet<>(set);
                rightRelation.remove(t);

                int totalCost = costMap.get(rightRelation) + sizeOfRelation(rightRelation);
                if (totalCost < lowestCost) {
                    // set lowest cost and new best plan
                    lowestCost = totalCost;
                    plan = new ArrayList<Operator>(bestPlan.get(rightRelation));
                    plan.add(t);
                }
            }

            // store cost
            costMap.put(set, lowestCost);
            // store optimal join order
            bestPlan.put(set, plan);
        }
    }

    /** TODO */
    private int sizeOfRelation(HashSet<Operator> plan) {
        // only consider equality conditions for joins (in unionfind)
        // if no equality conditions, act as cross product - size of relations
        // multiplied

        // calc size of single relation (either scan or select)
        if (plan.size() == 1) {
            DBCatalog db = DBCatalog.getInstance();
            Operator op = plan.iterator().next();
            String tableName = op.outputSchema.get(0).getTable().getName();
            int numTuples = Integer.valueOf(db.getStats().get(tableName).get(0));

            if (op instanceof ScanOperator) {
                // for base table, its just the # tuples in stats.txt
                sizeMap.put(plan, numTuples);
                return numTuples;
            } else {
                // compute the size of R as the size of the base table multiplied by the product
                // of all the reduction factors on all the attributes mentioned in the selection
                Table table = op.outputSchema.get(0).getTable();
                String name = table.getAlias() != null ? table.getAlias().getName() : table.getName();
                Collection<Double> reductionfactors = reductionInfo.get(name).values();
                int size = 1;
                for (Double num : reductionfactors) {
                    size *= num;
                }
                size *= numTuples;
                // check 0
                size = size > 0 ? size : 1;
                sizeMap.put(plan, size);
                return size;
            }
        }

        // get the relations joined on
        ArrayList<Operator> outerRelation = bestPlan.get(plan);
        int n = outerRelation.size();
        HashSet<Operator> left = new HashSet<>(outerRelation.subList(0, n - 1));
        HashSet<Operator> right = new HashSet<>(outerRelation.subList(n - 1, n));

        // get sizes of relations
        int sizeLeft = sizeMap.get(left);
        int sizeRight = sizeMap.get(right);

        // for attribute in corresponding unionfinds, calc v-values for each left and
        // right, get max(left, right), multiply all maxes
        int total = 1;
        for (Column col : right.iterator().next().outputSchema) {
            UFElement ufe;
            if ((ufe = uf.find(col)) != null) {
                List<String> attributes = ufe.getAttributes();
                HashMap<Operator, String> outerAttributes = new HashMap<>();
                for (Operator op : left) {
                    for (Column col2 : op.outputSchema) {
                        String tableName = col2.getTable().getAlias() != null ? col2.getTable().getAlias().getName()
                                : col2.getTable().getName();
                        if (attributes.contains(tableName + "." + col2.getColumnName())) {
                            outerAttributes.put(op, col2.getColumnName());
                        }
                    }
                }
                total *= Math.max(V(left, outerAttributes), V(right, col));
            }
        }

        // calc and store result
        // size of relations multipled / max of V values per equality attribute
        int result = sizeLeft * sizeRight / total;
        // check 0
        result = result > 0 ? result : 1;
        sizeMap.put(plan, result);
        return result;
    }

    /** TODO */
    private int V(HashSet<Operator> plan, Column attribute) {
        // for selections or two tables, adjust for # of tuples
        // no v-value is zero, round up to 1

        // single relation
        DBCatalog db = DBCatalog.getInstance();
        Operator op = plan.iterator().next();
        String tableName = op.outputSchema.get(0).getTable().getName();
        ArrayList<String> stats = db.getStats().get(tableName);

        // get min and max for attribute
        int aMin = Integer.MIN_VALUE;
        int aMax = Integer.MAX_VALUE;
        for (int i = 1; i < stats.size(); i += 3) {
            if (stats.get(i) == attribute.getColumnName()) {
                aMin = Integer.valueOf(stats.get(i + 1));
                aMax = Integer.valueOf(stats.get(i + 2));
                break;
            }
        }

        // calc range
        int vVal = aMax - aMin + 1;

        if (!(op instanceof ScanOperator)) {
            // if selection, max - min + 1 for attribute and adjust min and max for
            // reduction factors from selection
            vVal = vVal > sizeMap.get(plan) ? sizeMap.get(plan) : vVal;
        }

        // check for 0
        vVal = vVal > 0 ? vVal : 1;

        // add to map and return
        HashMap<String, Integer> planVMap = vMap.get(plan);
        if (planVMap != null) {
            planVMap.put(attribute.getColumnName(), vVal);
        } else {
            planVMap = new HashMap<>();
            planVMap.put(attribute.getColumnName(), vVal);
            vMap.put(plan, planVMap);
        }
        return vVal;
    }

    /** TODO */
    private int V(HashSet<Operator> plan, HashMap<Operator, String> attributes) {
        // 2 or more relations
        ArrayList<Integer> values = new ArrayList<>();
        for (Operator op : attributes.keySet()) {
            HashSet<Operator> temp = new HashSet<>();
            temp.add(op);
            values.add(vMap.get(temp).get(attributes.get(op)));
        }
        int vVal = Collections.min(values);

        // check 0 and clamp down
        vVal = vVal > sizeMap.get(plan) ? sizeMap.get(plan) : vVal > 0 ? vVal : 1;

        return vVal;
    }

    /** TOOD */
    private void createSubsets(ArrayList<Operator> tables, int subsetSize, int index, HashSet<Operator> inProgress,
            ArrayList<HashSet<Operator>> subsets) {
        if (inProgress.size() == subsetSize) {
            subsets.add(new HashSet<>(inProgress));
            return;
        }
        for (int i = index; i < tables.size(); i++) {
            inProgress.add(tables.get(i));
            createSubsets(tables, subsetSize, i + 1, inProgress, subsets);
            inProgress.remove(tables.get(i));
        }
    }
}