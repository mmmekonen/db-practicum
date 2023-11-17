package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import logical_operator.Join;

/** TODO */
public class DetermineJoinOrder {

    private ArrayList<String> allTables;
    // cost of best plan
    HashMap<HashSet<String>, Integer> costMap;
    // expected output size of each relation
    HashMap<HashSet<String>, Integer> sizeMap;
    // actual best plan order
    HashMap<HashSet<String>, ArrayList<String>> bestPlan;

    /** TODO */
    public DetermineJoinOrder(Join join) {
        // TODO: determine tables in the join

        // iterate through all subsets
        ArrayList<HashSet<String>> currSubsets = new ArrayList<>();
        for (int i = 1; i < allTables.size(); i++) {
            createSubsets(allTables, i, 0, new HashSet<>(), currSubsets);

            // loop through them
            for (HashSet<String> joinSet : currSubsets) {
                findJoinOrder(joinSet);
            }

            // reset for next size
            currSubsets.clear();
        }
    }

    /** TODO */
    private void findJoinOrder(HashSet<String> set) {
        // for size == 1, just relation, no cost
        if (set.size() == 1) {
            costMap.put(set, 0);
            sizeMap.put(set, null/** TODO: put size here */
            );
            bestPlan.put(set, new ArrayList<>(set));
        }

        // for size == 2, no cost
        else if (set.size() == 2) {
            costMap.put(set, 0);
            sizeMap.put(set, null/** TODO: put size here */
            );
            bestPlan.put(set, null /** TODO: put smallest on right side */
            );
        }

        // for size >= 3
        else {
            // get all possible left relations
            ArrayList<String> plan = new ArrayList<>();
            int lowestCost = Integer.MAX_VALUE;
            // loop through them to determine cost/best left relation
            for (String t : set) {
                HashSet<String> rightRelation = new HashSet<>(set);
                rightRelation.remove(t);

                int totalCost = cost(rightRelation) + sizeOfRelation(rightRelation);
                if (totalCost < lowestCost) {
                    // set lowest cost and new best plan
                    lowestCost = totalCost;
                    plan = new ArrayList<String>(bestPlan.get(rightRelation));
                    plan.add(t);
                }
            }

            // determine size
            sizeMap.put(set, 0/** TODO:put size of set */
            );
            // store cost
            costMap.put(set, lowestCost);
            // store optimal join order
            bestPlan.put(set, plan);
        }
    }

    /** TODO */
    private int cost(HashSet<String> plan) {
        // for < 3 relations = 0
        return 0;
    }

    /** TODO */
    private int sizeOfRelation(HashSet<String> plan) {
        // only consider equality conditions for joins
        // if no equality conditions, act as cross product - size of relations
        // multiplied
        return 0;
    }

    // get bounds from union find or stats file

    /** TODO */
    private int V(HashSet<String> plan) {
        // if plan.size() == 0, max - min + 1
        // adjust min and max for reduction factors from selection
        // or
        // compute the size of R as the size of the base table multiplied by the product
        // of all the reduction factors on all the attributes mentioned in the selection

        // for selection, adjust for # fo tuples

        // no v-value is zero, round up to 1

        // join of 2 other relations:

        return 0;
    }

    /** TOOD */
    private void createSubsets(ArrayList<String> tables, int subsetSize, int index, HashSet<String> inProgress,
            ArrayList<HashSet<String>> subsets) {
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