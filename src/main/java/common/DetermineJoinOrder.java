package common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import physical_operator.Operator;
import physical_operator.ScanOperator;

/**
 * A class to determine the optimal join order for joins in the current query.
 * Creates a left deep join plan with the lowest cost relations as the outside
 * relation. The algorithm starts with the smallest possible subsets for a join
 * (single tables) and works its way up until it finds the optimal join order
 * for the entire join. For a single table, the cost is zero and the optimal
 * join is just the table itself. For a join of two relations, the cost is also
 * zero and the optimal join order is with the smaller table as the outer
 * relation. The cost of each larger join is calculated as the cost of its
 * outer relation times the estimated size of the outer relation. The cost of
 * the outer relation is previously computed in a prior iteration of the
 * algorithm. To calculate the estimated size for each relation, if it is a base
 * table, then the size is the total number of tuples in the table. If it is a
 * selection, then the size is the number of tuples in the relation times the
 * product of all the reduction factors on that table. The size of any
 * combination of tables (a join) was determined using the size of the outer
 * table times the size of the inner table divided by the product of the maximum
 * V-Values calculated for attributes that had equality join conditions on each
 * other (as found in the given union-find). To calculate the V-Values, if the
 * relation was a base table, it used the max - min + 1 for the possible values
 * in that table for the specified attribute (found in stats.txt file). For a
 * selection, it's the same except that value is multiplied by the reduction
 * factors for that table. For any join of relations, the V-Value was calculated
 * as the minimum V-Value from all the attributes in that relation that are in
 * the equality condition. Finally, all V-Values are checked to make sure that
 * they are >= 1 and are <= the estimated size of each table.
 */
public class DetermineJoinOrder {

    private ArrayList<Operator> allTables;
    private SelectUF uf;
    private HashMap<String, HashMap<String, Double>> reductionInfo;
    // cost of best plan
    HashMap<HashSet<Operator>, Integer> costMap = new HashMap<>();
    // expected output size of each relation
    HashMap<HashSet<Operator>, Integer> sizeMap = new HashMap<>();
    // v-values for relations
    HashMap<HashSet<Operator>, HashMap<String, Integer>> vMap = new HashMap<>();
    // actual best plan order
    HashMap<HashSet<Operator>, ArrayList<Operator>> bestPlan = new HashMap<>();

    /**
     * Creates a DetermineJoinOrder object that creates the optimal join order using
     * the given child operators, a union-find, and the reduction factors for those
     * operators.
     * 
     * @param ops           An ArrayList of operators.
     * @param unionFind     A SelectUF object.
     * @param reductionInfo A HashMap of the reduction factors for the given
     *                      operators.
     */
    public DetermineJoinOrder(ArrayList<Operator> ops, SelectUF unionFind,
            HashMap<String, HashMap<String, Double>> reductionInfo) {
        allTables = ops;
        uf = unionFind;
        this.reductionInfo = reductionInfo;

        // iterate through all subsets
        ArrayList<HashSet<Operator>> currSubsets = new ArrayList<>();
        for (int i = 1; i <= allTables.size(); i++) {
            createSubsets(allTables, i, 0, new HashSet<>(), currSubsets);

            // loop through them
            for (HashSet<Operator> joinSet : currSubsets) {
                findJoinOrder(joinSet);
            }

            // reset for next size
            currSubsets.clear();
        }
    }

    /**
     * Returns the final join order.
     * 
     * @return an ArrayList of operators.
     */
    public ArrayList<Operator> finalOrder() {
        return bestPlan.get(new HashSet<>(allTables));
    }

    /**
     * Finds the optimal join order for the given set of operators.
     * 
     * @param set a HashSet of operators.
     */
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
                if (sizeOfRelation(temp) < lowest) {
                    best.add(0, t);
                    lowest = sizeMap.get(temp);
                } else {
                    best.add(t);
                }
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
                HashSet<Operator> leftRelation = new HashSet<>(set);
                leftRelation.remove(t);

                int totalCost = costMap.get(leftRelation) + sizeOfRelation(leftRelation);
                if (totalCost < lowestCost) {
                    // set lowest cost and new best plan
                    lowestCost = totalCost;
                    plan = new ArrayList<Operator>(bestPlan.get(leftRelation));
                    plan.add(t);
                }
            }

            // store cost
            costMap.put(set, lowestCost);
            // store optimal join order
            bestPlan.put(set, plan);
        }
    }

    /**
     * Calculates and stores the size of the given relation.
     * 
     * @param plan a HashSet of operators representing a relation.
     * @return an int of the estimated size of the relation.
     */
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
                HashMap<String, Double> factorsByCol = reductionInfo.get(name);
                Collection<Double> reductionfactors = factorsByCol != null ? factorsByCol.values() : new ArrayList<>(0);
                int size = 1;
                for (Double num : reductionfactors) {
                    size *= num;
                }
                size *= numTuples;
                // check 0
                size = size >= 1 ? size : 1;
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
        ArrayList<Column> schema = right.iterator().next().outputSchema;
        for (Column col : schema) {
            UFElement ufe;
            if ((ufe = uf.find(col)) != null) {
                List<String> attributes = ufe.getAttributes();
                HashMap<Operator, String> outerAttributes = new HashMap<>();
                for (Operator op : left) {
                    for (Column col2 : op.outputSchema) {
                        if (attributes.contains(col2.toString())) {
                            outerAttributes.put(op, col2.getColumnName());
                        }
                    }
                }
                if (outerAttributes.keySet().size() != 0) {
                    total *= Math.max(V(left, outerAttributes), V(right, col.getColumnName()));
                }
            }
        }

        // calc and store result
        // size of relations multipled / max of V values per equality attribute
        int result = (sizeLeft * sizeRight) / total;
        // check 0
        result = result >= 1 ? result : 1;
        sizeMap.put(plan, result);
        return result;
    }

    /**
     * Calculates and stores the V-Value of the given operator and attribute.
     * This method is meant to be used for single tables only.
     * 
     * @param plan      a HashSet of an Operator.
     * @param attribute a String of the attribute to calculate the V-Value for.
     * @return an int of the V-Value.
     */
    private int V(HashSet<Operator> plan, String attribute) {
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
            if (stats.get(i) == attribute) {
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
        vVal = vVal >= 1 ? vVal : 1;

        // add to map and return
        HashMap<String, Integer> planVMap = vMap.get(plan);
        if (planVMap != null) {
            planVMap.put(attribute, vVal);
        } else {
            planVMap = new HashMap<>();
            planVMap.put(attribute, vVal);
            vMap.put(plan, planVMap);
        }
        return vVal;
    }

    /**
     * Calculates and stores the V-Value of the given operator and attribute.
     * This method is meant to be used for joined relations only.
     * 
     * @param plan       a HashSet of Operators.
     * @param attributes a HashMap<Operator,String> for the attributes from the
     *                   given relation.
     * @return an int of the V-Value.
     */
    private int V(HashSet<Operator> plan, HashMap<Operator, String> attributes) {
        // 2 or more relations
        ArrayList<Integer> values = new ArrayList<>();
        for (Operator op : attributes.keySet()) {
            HashSet<Operator> temp = new HashSet<>();
            temp.add(op);
            if (vMap.get(temp) != null && vMap.get(temp).get(attributes.get(op)) != null) {
                values.add(vMap.get(temp).get(attributes.get(op)));
            } else {
                values.add(V(temp, attributes.get(op)));
            }
        }

        // get min vValue
        int vVal = Collections.min(values);

        // check 0 and clamp down
        vVal = vVal > sizeMap.get(plan) ? sizeMap.get(plan) : vVal > 0 ? vVal : 1;

        return vVal;
    }

    /**
     * Creates a subset of a certain size using the given tables.
     * 
     * @param tables     an ArrayList of operators to use for subsets.
     * @param subsetSize the size of the subsets to make.
     * @param index      the current index in tables.
     * @param inProgress a HashSet of operators that is currently being made.
     * @param subsets    an ArrayList of HashSets to store the subsets in.
     */
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