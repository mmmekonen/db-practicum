package operator;

import common.DBCatalog;
import common.Tuple;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import net.sf.jsqlparser.expression.Alias;

/*
 * A class to represent the a scan operator on a table. ScanOperators scan each tuple in a file and
 * return them. The file is specified by the table name. The output schema is the schema of the
 * table.
 */
public class ScanOperator extends Operator {

  private File dbFile;
  private Scanner scanner;

  // explicit constructor
  /*
   * Creates a scan operator using a table name and an alias. For each tuple in
   * the table, the
   * scan operator returns a tuple with the same schema as the table.
   *
   * @param tableName The name of the table to scan.
   * @ param alias The alias of the table.
   */
  public ScanOperator(String tableName, Alias alias) {

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
