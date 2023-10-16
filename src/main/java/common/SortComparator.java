package common;

import java.util.Comparator;
import java.util.HashMap;

/**
 * A class to represent a sort comparator on Tuples. Sorts each tuple based on
 * the given sort order.
 */
public class SortComparator implements Comparator<Tuple> {

  private HashMap<Integer, Integer> sortOrder;

  /**
   * Creates a sort comparator that compares two tuples using the given sort
   * order.
   *
   * @param sortOrder A HashMap of the final order position to the position of the
   *                  element in the
   *                  tuple that belongs in that order position.
   */
  public SortComparator(HashMap<Integer, Integer> sortOrder) {
    super();
    this.sortOrder = sortOrder;
  }

  /**
   * Compares two tuples element-by-element. If one of the tuples are null, the
   * other tuple is considered to be smaller. If both are null, they are equal.
   *
   * @param o1 One of the tuples to be compared
   * @param o2 One of the tuples to be compared
   * @return
   */
  @Override
  public int compare(Tuple o1, Tuple o2) {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return 1;
    } else if (o2 == null) {
      return -1;
    }
    for (int i = 0; i < this.sortOrder.size(); i++) {
      // keep comparing each tuple element until they aren't equal
      int comp = Integer.compare(
          o1.getElementAtIndex(this.sortOrder.get(i)),
          o2.getElementAtIndex(this.sortOrder.get(i)));
      if (comp != 0) {
        return comp;
      }
    }
    return 0;
  }
}
