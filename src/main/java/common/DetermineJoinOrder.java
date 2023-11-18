package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import logical_operator.Join;

/** TODO */
public class DetermineJoinOrder {

    private ArrayList<String> allTables;
    private SelectUF uf;
    // cost of best plan
    HashMap<HashSet<String>, Integer> costMap;
    // expected output size of each relation
    HashMap<HashSet<String>, Integer> sizeMap;
    // actual best plan order
    HashMap<HashSet<String>, ArrayList<String>> bestPlan;

    /** TODO */
    public DetermineJoinOrder(Join join) {
        // TODO: determine tables in the join

        // TODO: get unionfind from join

        // TODO: get reduction factors?

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
            bestPlan.put(set, new ArrayList<>(set));
        }

        // for size == 2, no cost
        else if (set.size() == 2) {
            costMap.put(set, 0);

            // calc size of both tables
            ArrayList<String> best = new ArrayList<>();
            int lowest = Integer.MAX_VALUE;
            for (String t : set) {
                HashSet<String> temp = new HashSet<>();
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
            ArrayList<String> plan = new ArrayList<>();
            int lowestCost = Integer.MAX_VALUE;
            // get all possible left relations and
            // loop through them to determine cost/best left relation
            for (String t : set) {
                HashSet<String> rightRelation = new HashSet<>(set);
                rightRelation.remove(t);

                int totalCost = costMap.get(rightRelation) + sizeOfRelation(rightRelation);
                if (totalCost < lowestCost) {
                    // set lowest cost and new best plan
                    lowestCost = totalCost;
                    plan = new ArrayList<String>(bestPlan.get(rightRelation));
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
    private int sizeOfRelation(HashSet<String> plan) {
        // only consider equality conditions for joins (in unionfind)
        // if no equality conditions, act as cross product - size of relations
        // multiplied

        // size of relations multipled / max of V values per equality attribute

        // calc size of single relation (either scan or select)
        if (plan.size() == 1) {
            // TODO: compute the size of R as the size of the base table multiplied by the
            // product
            // of all the reduction factors on all the attributes mentioned in the selection

            // for base table, its just the # tuples in stats.txt

        }

        // get the relations joined on
        ArrayList<String> outerRelation = bestPlan.get(plan);
        int n = outerRelation.size();
        List<String> left = outerRelation.subList(0, n - 1);
        List<String> right = outerRelation.subList(plan.size() - 1, n);

        // get sizes of relations
        int sizeLeft = sizeMap.get(new HashSet<>(left));
        int sizeRight = sizeMap.get(new HashSet<>(right));

        // TODO: compute v values
        // for attribute in corresponding unionfinds
        // calc v-values for each left and right
        // get max(left, right)
        // multiply all maxes

        int total = 1;

        // calc and store result
        int result = sizeLeft * sizeRight / total;
        sizeMap.put(plan, result);
        return result;
    }

    // TODO: get bounds from union find or stats file

    /** TODO */
    private int V(HashSet<String> plan) {
        // if base table, not selection, max - min + 1 for attribute
        // adjust min and max for reduction factors from selection

        // for selections or two tables, adjust for # of tuples

        // no v-value is zero, round up to 1

        // join of 2 other relations:

        // TODO: calc v-values
        if (plan.size() == 1) {
            // if base
            {
            }
            // if selection
            {
            }
        } else {
            // 2 or more relations

        }

        return 1;
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