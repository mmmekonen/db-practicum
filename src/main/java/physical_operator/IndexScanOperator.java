package physical_operator;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Alias;
import common.Tuple;
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
  public int[] currentDataEntries;
  public int dataEntryIndex;

  public IndexScanOperator(String tableName, Alias alias, String indexColumnName, Integer lowkey, Integer highkey) {
    super(null);
    this.lowkey = lowkey;
    this.highkey = highkey;
    this.scanner = new ScanOperator(tableName, alias);

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
        while (dataEntryIndex < currentDataEntries.length) {
          int key = currentDataEntries[dataEntryIndex];
          int recordID = currentDataEntries[dataEntryIndex + 1];

          if (key >= lowkey && key <= highkey) {
            Tuple tuple = getTupleFromRecordID(tree, recordID);

            tuplePosition++;
            dataEntryIndex += 2;
            return tuple;
          } else if (key > highkey) {
            break;
          } else {
            dataEntryIndex += 2;
          }
        }

        currentLeaf = tree.getNextLeaf();

        if (currentLeaf != null) {
          currentDataEntries = extractDataEntries(currentLeaf);
          dataEntryIndex = 0;
        }
      }
    }

    return null;
  }

  private Tuple getTupleFromRecordID(TreeIndex tree2, int recordID) {
    return null;
  }

  @Override
  public void reset() {
    super.reset(0);
  }

  public int[] extractDataEntries(int[] leafNode) {
    if (leafNode != null && leafNode[0] == 0) {
      int numDataEntries = leafNode[1];
      int[] dataEntries = new int[numDataEntries * 2];

      for (int i = 0; i < numDataEntries; i++) {
        int dataIndex = 2 + (i * 2);
        dataEntries[i * 2] = leafNode[dataIndex];
        dataEntries[i * 2 + 1] = leafNode[dataIndex + 1];
      }
      return dataEntries;
    }
    return null;
  }

}
