package physical_operator;

import common.DBCatalog;
import common.Tuple;
import common.TupleReader;
import java.io.File;
import java.io.IOException;
import net.sf.jsqlparser.expression.Alias;

/**
 * A class to represent the a scan operator on a table. ScanOperators scan each tuple in a file and
 * return them. The file is specified by the table name. The output schema is the schema of the
 * table.
 */
public class ScanOperator extends Operator {

  private File dbFile;
  private TupleReader reader;

  // explicit constructor
  /**
   * Creates a scan operator using a table name and an alias. For each tuple in the table, the scan
   * operator returns a tuple with the same schema as the table.
   *
   * @param tableName The name of the table to scan.
   * @param alias The alias of the table.
   */
  public ScanOperator(String tableName, Alias alias) {

    super(DBCatalog.getInstance().getTableSchema(tableName));

    DBCatalog dbCatalog = DBCatalog.getInstance();
    this.dbFile = dbCatalog.getFileForTable(tableName);

    try {
      this.reader = new TupleReader(dbFile);
    } catch (IOException e) {
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
      reader.close();
      reader = new TupleReader(dbFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Get next tuple from operator
   *
   * @return next Tuple, or null if we are at the end
   */
  public Tuple getNextTuple() {
    try {
      Tuple t = reader.readNextTuple();
      return t;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}