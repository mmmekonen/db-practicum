package operator;

import common.DBCatalog;
import common.Tuple;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class ScanOperator extends Operator {

  private File dbFile;
  private Scanner scanner;
  private ArrayList<Tuple> tuples = new ArrayList<Tuple>();
  private int index = 0;

  // explicit constructor
  public ScanOperator(String tableName) {

    // change
    super(DBCatalog.getInstance().getTableSchema(tableName));

    DBCatalog dbCatalog = DBCatalog.getInstance();
    this.dbFile = dbCatalog.getFileForTable(tableName);

    try {
      this.scanner = new Scanner(dbFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  /** Resets cursor on the operator to the beginning */
  public void reset() {
    // reset to beginning
    try {
      this.scanner = new Scanner(this.dbFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public Tuple getNextTuple() {
    while (this.scanner.hasNextLine()) {
      String line = this.scanner.nextLine();
      Tuple t = new Tuple(line);
      return t;
    }
    return null;
  }
}
