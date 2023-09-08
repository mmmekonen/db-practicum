package operator;

import common.DBCatalog;
import common.Tuple;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class ScanOperator extends Operator {

  private File dbFile;
  private ArrayList<Tuple> tuples = new ArrayList<Tuple>();
  private int index = 0;

  // explicit constructor
  public ScanOperator(String tableName) {

    // change
    super(DBCatalog.getInstance().getTableSchema(tableName));

    DBCatalog dbCatalog = DBCatalog.getInstance();
    this.dbFile = dbCatalog.getFileForTable(tableName);
    this.index = 0;

    Scanner sc;
    try {
      sc = new Scanner(dbFile);
      while (sc.hasNextLine()) {
        String line = sc.nextLine();
        Tuple t = new Tuple(line);
        this.tuples.add(t);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  /** Resets cursor on the operator to the beginning */
  public void reset() {
    // reset to beginning
    this.index = 0;
  }

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public Tuple getNextTuple() {
    while (this.tuples.size() > this.index) {
      this.index++;
      return this.tuples.get(this.index - 1);
    }
    return null;
  }
}
