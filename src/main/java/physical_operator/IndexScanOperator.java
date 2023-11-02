package physical_operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Alias;
import common.Tuple;
import common.TupleReader;
import common.TreeIndex;
import common.DBCatalog;

public class IndexScanOperator extends Operator {
  public Integer lowkey;
  public Integer highkey;
  public TreeIndex tree;
  public boolean clustered;
  public int tuplePosition;
  public ScanOperator scanner;
  public int[] currentLeaf;
  public TupleReader reader;
  public int dataEntryIndex;
  public int ridIndex;
  ArrayList<ArrayList<Integer>> dataEntries;

  public IndexScanOperator(String tableName, Alias alias, String indexColumnName, Integer lowkey, Integer highkey) {
    super(DBCatalog.getInstance().getTableSchema(tableName));
    this.lowkey = lowkey;
    this.highkey = highkey;
    this.scanner = new ScanOperator(tableName, alias);

    // FIX THE READER INPUT
    try {
      this.reader = new TupleReader(DBCatalog.getInstance().getFileForTable(tableName));
    } catch (IOException e) {
      e.printStackTrace();
    }

    DBCatalog catalog = DBCatalog.getInstance();
    HashMap<String, ArrayList<String>> c = catalog.getIndexInfo();
    ArrayList<String> indexInfo = c.get(tableName);
    this.clustered = (indexInfo.get(1) == "1");

    this.tree = new TreeIndex(catalog.getIndexDirectory() + '/' + tableName + '.' + indexColumnName);
    this.tuplePosition = 0;

    if (lowkey == null) {
      currentLeaf = tree.readNode(1);
    } else {
      currentLeaf = tree.deserialize(1, lowkey);
      dataEntries = tree.getDataEntries();
      ridIndex = 0;
      tuplePosition = lowkey;

      while (dataEntries.get(dataEntryIndex).get(0) < lowkey) {
        dataEntryIndex++;
      }

    }

    for (int i = 0; i < this.outputSchema.size(); i++) {
      this.outputSchema.get(i).getTable().setAlias(alias);
    }
  }

  public Tuple getNextTuple() {
    if (clustered) {
      Tuple tuple = scanner.getNextTuple();
      if (tuple != null && tuplePosition >= lowkey && tuplePosition <= highkey) {
        tuplePosition++;
        return tuple;
      }
    } else {
      if (currentLeaf != null) {
        while (dataEntryIndex < dataEntries.size()) {
          int key = dataEntries.get(dataEntryIndex).get(0);
          int pageID = dataEntries.get(dataEntryIndex).get(1 + ridIndex * 2);
          int tupleID = dataEntries.get(dataEntryIndex).get(2 + ridIndex * 2);
          ridIndex++;

          if (key >= lowkey && key <= highkey) {
            Tuple tuple = reader.getTupleByPosition(pageID, tupleID);

            tuplePosition++;
            dataEntryIndex += 1;
            return tuple;
          } else if (key > highkey) {
            break;
          } else {
            dataEntryIndex += 1;
          }
        }

        currentLeaf = tree.getNextLeaf();

        if (currentLeaf != null) {
          dataEntries = tree.getDataEntries();
          dataEntryIndex = 0;
          ridIndex = 0;
        }
      }
    }

    return null;
  }

  @Override
  public void reset() {
    super.reset(0);
  }

}
