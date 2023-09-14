package operator;

import common.DBCatalog;
import common.QueryPlanBuilder;
import common.Tuple;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.schema.Column;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class ScanOperator extends Operator {

  private File dbFile;
  private Scanner scanner;

  // explicit constructor
  public ScanOperator(String tableName, Alias alias) {

    // change
    super(DBCatalog.getInstance().getTableSchema(tableName));

    DBCatalog dbCatalog = DBCatalog.getInstance();
    this.dbFile = dbCatalog.getFileForTable(tableName);

    try {
      this.scanner = new Scanner(dbFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    for (int i = 0; i < this.outputSchema.size(); i++) {
      this.outputSchema.get(i).getTable().setAlias(alias);
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
