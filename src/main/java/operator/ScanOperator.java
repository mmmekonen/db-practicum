package operator;

import common.DBCatalog;
import common.Tuple;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class ScanOperator extends Operator {

  private String tableName;
  private File dbFile;
  private ArrayList<Tuple> tuples = new ArrayList<Tuple>();

  // explicit constructor
  public ScanOperator(String tableName) {

    // change
    super(null);

    this.tableName = tableName;
    DBCatalog dbCatalog = DBCatalog.getInstance();
    this.dbFile = dbCatalog.getFileForTable(tableName);

    Scanner sc;
    try {
      sc = new Scanner(dbFile);
      while (sc.hasNextLine()) {
        String line = sc.nextLine();
        String[] lineArr = line.split("\\|");
        ArrayList<Integer> tuple = new ArrayList<Integer>();
        for (String s : lineArr) {
          tuple.add(Integer.parseInt(s));
        }
        Tuple t = new Tuple(tuple);
        this.tuples.add(t);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  /** Resets cursor on the operator to the beginning */
  public void reset() {
    // TODO
    // reset to beginning
    new ScanOperator(this.tableName);
  }

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public Tuple getNextTuple() {
    while (this.tuples.size() > 0) {
      return this.tuples.remove(0);
    }
    return null;
  }
}
