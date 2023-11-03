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
  public boolean notEndReached;

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
    this.notEndReached = true;

    System.out.println("STARTING");
    System.out.println("");

    System.out.println("Low key: " + lowkey);
    System.out.println("High key: " + highkey);

    int[] header = tree.readNode3(0);
    int rootPageID = header[0];
    System.out.println("Root page ID: " + rootPageID);

    if (highkey == null) {
      this.highkey = Integer.MAX_VALUE;
    }

    if (lowkey == null) {
      currentLeaf = tree.readNode3(1);
    } else {
      currentLeaf = tree.deserialize(rootPageID, lowkey);
      dataEntries = tree.getDataEntries();
      ridIndex = 0;
      tuplePosition = lowkey;

      // System.out.println("curr page" + tree.curPage);
      // System.out.println(dataEntries);

      while (dataEntries.get(dataEntryIndex).get(0) < lowkey) {
        dataEntryIndex++;
      }

    }

    for (int i = 0; i < this.outputSchema.size(); i++) {
      this.outputSchema.get(i).getTable().setAlias(alias);
    }
  }

  public Tuple getNextTuple() {

    while (notEndReached) {
      if (clustered) {
        Tuple tuple = scanner.getNextTuple();
        if (tuple != null && tuplePosition >= lowkey && tuplePosition <= highkey) {
          tuplePosition++;
          return tuple;
        }
      } else {
        if (currentLeaf != null) {
          while (dataEntryIndex < dataEntries.size()) {

            // System.out.println("data entry index: " + dataEntryIndex);
            // System.out.println("ridIndex" + ridIndex);

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

              System.out.println("TUPLLLEEE");
              System.out.println(tuple);
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

          System.out.println("CALLING GET NEXT LEAF");
          currentLeaf = tree.getNextLeaf();
          System.out.println("GOT NEXT LEAF");

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

  @Override
  public void reset() {
    super.reset(0);
  }

}
