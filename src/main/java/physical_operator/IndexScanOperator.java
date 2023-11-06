package physical_operator;

import common.DBCatalog;
import common.TreeIndex;
import common.Tuple;
import common.TupleReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.schema.Column;

/** A class to represent an index scan operator on a relation. */
public class IndexScanOperator extends Operator {
  public Integer lowkey;
  public Integer highkey;
  public TreeIndex tree;
  public boolean clustered;
  public ScanOperator scanner;
  public int[] currentLeaf;
  public TupleReader reader;
  public int dataEntryIndex;
  public int ridIndex;
  ArrayList<ArrayList<Integer>> dataEntries;
  public boolean notEndReached;
  public int indexColumnInt;

  /** Creates a new IndexScanOperator. */
  public IndexScanOperator(
      String tableName, Alias alias, String indexColumnName, Integer lowkey, Integer highkey) {
    super(DBCatalog.getInstance().getTableSchema(tableName));
    this.lowkey = lowkey;
    this.highkey = highkey;
    this.scanner = new ScanOperator(tableName, alias);

    ArrayList<Column> cols = DBCatalog.getInstance().getTableSchema(tableName);
    ArrayList<String> colNames = new ArrayList<String>();

    for (int i = 0; i < cols.size(); i++) {
      colNames.add(cols.get(i).getColumnName());
    }

    this.indexColumnInt = colNames.indexOf(indexColumnName);

    try {
      this.reader = new TupleReader(DBCatalog.getInstance().getFileForTable(tableName));
    } catch (IOException e) {
      e.printStackTrace();
    }

    DBCatalog catalog = DBCatalog.getInstance();
    HashMap<String, ArrayList<String>> c = catalog.getIndexInfo();
    ArrayList<String> indexInfo = c.get(tableName);
    this.clustered = (indexInfo.get(1).equals("1"));

    this.tree =
        new TreeIndex(catalog.getIndexDirectory() + '/' + tableName + '.' + indexColumnName);
    this.notEndReached = true;

    int[] header = tree.readNode3(0);
    int rootPageID = header[0];

    if (highkey == null) {
      this.highkey = Integer.MAX_VALUE;
    }

    if (lowkey == null) {
      currentLeaf = tree.readNode3(1);
    } else {
      currentLeaf = tree.deserialize(rootPageID, lowkey);
      dataEntries = tree.getDataEntries();
      ridIndex = 0;

      while (dataEntries.get(dataEntryIndex).get(0) < lowkey) {
        dataEntryIndex++;
      }
    }

    for (int i = 0; i < this.outputSchema.size(); i++) {
      this.outputSchema.get(i).getTable().setAlias(alias);
    }
  }

  /**
   * Gets the next tuple from the operator.
   *
   * @return the next tuple.
   */
  public Tuple getNextTuple() {

    while (notEndReached) {
      if (clustered) {
        Tuple tuple = scanner.getNextTuple();

        while (tuple != null && tuple.getElementAtIndex(indexColumnInt) < lowkey) {
          tuple = scanner.getNextTuple();
        }

        if (tuple == null) {
          return null;
        } else if (tuple.getElementAtIndex(indexColumnInt) > highkey) {
          return null;
        } else {
          return tuple;
        }
      } else {
        if (currentLeaf != null) {
          while (dataEntryIndex < dataEntries.size()) {
            int key = dataEntries.get(dataEntryIndex).get(0);
            int pageID = dataEntries.get(dataEntryIndex).get(1 + ridIndex * 2);
            int tupleID = dataEntries.get(dataEntryIndex).get(2 + ridIndex * 2);

            if (key >= lowkey && key <= highkey) {
              Tuple tuple = reader.getTupleByPosition(pageID, tupleID);
              ridIndex++;

              if (ridIndex >= ((dataEntries.get(dataEntryIndex).size() - 1) / 2)) {
                dataEntryIndex++;
                ridIndex = 0;
              }

              return tuple;
            } else if (key > highkey) {
              return null;
            } else {
              if (ridIndex >= ((dataEntries.get(dataEntryIndex).size() - 1) / 2)) {
                dataEntryIndex++;
                ridIndex = 0;
              } else {
                ridIndex++;
              }
            }
          }

          currentLeaf = tree.getNextLeaf();

          if (currentLeaf != null) {
            if (currentLeaf[0] == 1) {
              return null;
            } else {
              dataEntries = tree.getDataEntries();
              dataEntryIndex = 0;
              ridIndex = 0;
            }
          } else if (currentLeaf == null) {
            return null;
          }

        } else {
          return null;
        }
      }
    }

    return null;
  }

  /** Resets the operator to the beginning. */
  @Override
  public void reset() {
    super.reset(0);
  }
}
