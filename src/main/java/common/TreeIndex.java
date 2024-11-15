package common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import physical_operator.InMemorySortOperator;
import physical_operator.Operator;
import physical_operator.ScanOperator;

public class TreeIndex {

  File file;
  FileOutputStream fileOutputStream;
  public FileChannel fileChannel;
  public ByteBuffer buffer;
  Tuple nextTuple;
  static final int PAGE_SIZE = 4096;
  public int curPage;
  public int[] curDataEntry;
  public boolean isIndex;
  int order;

  /**
   * Class to create and read a B+ tree index
   *
   * @param inputDir The input directory, which contains information such as the tables and index
   *     configurations
   * @param tableName The name of the table to be indexed
   * @param colName The name of the column to be indexed
   * @param indexElement The element on which the table will be indexed
   * @param info An arraylist containing the config data for the index
   */
  public TreeIndex(
      String inputDir,
      String tableName,
      String colName,
      int indexElement,
      ArrayList<Integer> info) {

    this.file = new File(inputDir + "/db/indexes/" + tableName + "." + colName);
    this.order = info.get(1);

    ScanOperator base = new ScanOperator(tableName, null);
    ArrayList<Column> schema = new ArrayList<>();
    schema.add(new Column(new Table(null, tableName), colName));
    InMemorySortOperator op = new InMemorySortOperator(base, schema);

    if (info.get(0) == 1) sortTable(op, inputDir + "/db/data/" + tableName);

    base = new ScanOperator(tableName, null);
    op = new InMemorySortOperator(base, schema);

    this.nextTuple = op.getNextTuple();

    ArrayList<ArrayList<Integer>> recordList = new ArrayList<>();
    ArrayList<Integer> nextRecord = makeRecord(op, indexElement);
    while (nextRecord != null) {
      recordList.add(nextRecord);
      nextRecord = makeRecord(op, indexElement);
    }
    Iterator<ArrayList<Integer>> data = recordList.iterator();
    int remaining = recordList.size();

    try {
      file.getParentFile().mkdirs();
      file.createNewFile();
      fileOutputStream = new FileOutputStream(file);
      fileChannel = fileOutputStream.getChannel();
      this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    } catch (Exception e) {
      System.out.println("you shouldn't see this, ever");
      e.printStackTrace();
    }

    ArrayList<int[]> leaves = new ArrayList<>();

    while (data.hasNext()) {
      if (2 * order < remaining && remaining < 3 * order) {
        leaves.add(LeafNode(data, remaining / 2));
        remaining -= remaining / 2;
        leaves.add(LeafNode(data, remaining));
        break;
      } else {
        leaves.add(LeafNode(data, 2 * order));
        remaining -= 2 * order;
      }
    }

    ArrayList<int[]> nodes = new ArrayList<>();
    nodes.add(new int[1]);
    nodes.addAll(leaves);
    indexNodeHelper(leaves, nodes, order);

    nodes.set(0, headerNode(nodes.size() - 1, leaves.size(), order));

    for (int i = 0; i < nodes.size(); i++) {
      try {
        writeNode(nodes.get(i));
      } catch (IOException e) {
        System.out.println("Tree loading failed");
      }
    }
  }

  /**
   * A class to read from a tree index
   *
   * @param filename The file from which the tree will be read
   */
  public TreeIndex(String filename) {
    this.file = new File(filename);

    try {
      fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ); // Open in read mode
      this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    } catch (IOException e) {
      e.printStackTrace(); // Handle the exception properly
    }
  }

  /**
   * A helper function that sorts a relation, primarily so that a clustered index can later be built
   * on it
   *
   * @param op A sort operator which will be used to sort the table
   * @param filepath The filepath of the table to be sorted
   */
  public static void sortTable(InMemorySortOperator op, String filepath) {
    try {
      TupleWriter t = new TupleWriter(filepath);
      Tuple next = op.getNextTuple();
      while (next != null) {
        t.writeTuple(next);
        next = op.getNextTuple();
      }
      t.close();
      op.reset();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Reads the node stored in the specified page
   *
   * @param page the page to be read
   * @return The data in a page, represented as an array of integers
   */
  public int[] readNode3(int page) {
    int[] result = new int[PAGE_SIZE / 4];

    try {
      buffer.clear();
      fileChannel.position(page * PAGE_SIZE);
      fileChannel.read(buffer);
      buffer.flip();

      buffer.asIntBuffer().get(result);
    } catch (IOException e) {
      e.printStackTrace();
    }

    this.curPage = page;
    this.curDataEntry = result;

    return result;
  }

  /**
   * Deserializes a node
   *
   * @param page The page to be deserialized
   * @param traverseKey The key to be used to traverse the tree
   * @return The data in a page, represented as an array of integers
   */
  public int[] deserialize(int page, Integer traverseKey) {
    readNode3(page);
    if (buffer.getInt(0) == 1) {
      return deserializeIndex(page, traverseKey);
    } else {
      return deserializeLeaf(page);
    }
  }

  /**
   * Gets the next leaf node in the tree
   *
   * @return The data in a page, represented as an array of integers
   */
  public int[] getNextLeaf() {
    return readNode3(curPage + 1);
  }

  /**
   * Deserializes an index node
   *
   * @param page The page to be deserialized
   * @param traverseKey The key to be used to traverse the tree
   * @return The data in a page, represented as an array of integers
   */
  public int[] deserializeIndex(int page, Integer traverseKey) {
    int numKeys = buffer.getInt(4);

    int[] keys = new int[numKeys];
    int[] children = new int[numKeys + 1];

    int pos = 8;
    for (int i = 0; i < numKeys; i++) {
      keys[i] = buffer.getInt(pos);
      pos += 4;
    }

    for (int i = 0; i < numKeys + 1; i++) {
      children[i] = buffer.getInt(pos);
      pos += 4;
    }

    int index = numKeys;
    for (int i = numKeys - 1; i >= 0; i--) {
      if (traverseKey < keys[i]) {
        index = i;
      }
    }

    return deserialize(children[index], traverseKey);
  }

  /**
   * Deserializes a leaf node
   *
   * @param page The page to be deserialized
   * @return The data in a page, represented as an array of integers
   */
  private int[] deserializeLeaf(int page) {
    List<Integer> keys = new ArrayList<>();
    List<List<Integer>> entries = new ArrayList<>();

    int pos = 4;
    int numDataEntries = buffer.getInt(pos);
    pos += 4;

    for (int i = 0; i < numDataEntries; i++) {
      int key = buffer.getInt(pos);
      pos += 4;
      keys.add(key);

      int numRids = buffer.getInt(pos);
      pos += 4;

      List<Integer> rids = new ArrayList<>();
      for (int j = 0; j < numRids; j++) {
        int pageNum = buffer.getInt(pos);
        pos += 4;
        int tupleNum = buffer.getInt(pos);
        pos += 4;

        rids.add(pageNum);
        rids.add(tupleNum);
      }

      entries.add(rids);
    }

    ArrayList<Integer> tempRes = new ArrayList<>();

    tempRes.add(page);
    tempRes.add(numDataEntries);

    for (int i = 0; i < keys.size(); i++) {
      tempRes.add(keys.get(i));
      tempRes.add(entries.get(i).size() / 2);
      tempRes.addAll(entries.get(i));
    }

    int[] result = new int[tempRes.size()];
    for (int i = 0; i < tempRes.size(); i++) {
      result[i] = tempRes.get(i);
    }

    this.curDataEntry = result;
    return result;
  }

  /**
   * Gets the data entries from a leaf node
   *
   * @return The data entries from a leaf node
   */
  public ArrayList<ArrayList<Integer>> getDataEntries() {
    int numDataEntries = curDataEntry[1];

    ArrayList<ArrayList<Integer>> result = new ArrayList<>();
    int index = 2;
    for (int i = 0; i < numDataEntries; i++) {
      int key = curDataEntry[index];
      index += 1;
      int numRids = curDataEntry[index];
      index += 1;

      ArrayList<Integer> rids = new ArrayList<>();
      rids.add(key);
      for (int j = 0; j < numRids; j++) {
        int pageNum = curDataEntry[index];
        index += 1;
        int tupleNum = curDataEntry[index];
        index += 1;

        rids.add(pageNum);
        rids.add(tupleNum);
      }

      result.add(rids);
    }

    // for (ArrayList<Integer> i : result) {
    // System.out.println(i);
    // }

    return result;
  }

  /**
   * Helper function to recursively create the index nodes of the table
   *
   * @param children The layer of nodes beneath the ones to be created
   * @param nodes A master list of nodes that tracks the entire tree
   * @param order The order of the tree
   * @return A newly created layer of nodes, represented as an arraylist
   */
  private ArrayList<int[]> indexNodeHelper(
      ArrayList<int[]> children, ArrayList<int[]> nodes, int order) {
    ArrayList<int[]> result = new ArrayList<>();
    int childrenStart = nodes.size() - children.size();

    for (int i = 0; i < children.size(); i += 2 * order + 1) {
      if (2 * order + 1 < children.size() - i && children.size() - i < 3 * order + 2) {
        int[] n = indexNode(nodes, childrenStart + i, (children.size() - i) / 2);
        nodes.add(n);
        result.add(n);
        int[] m =
            indexNode(
                nodes,
                childrenStart + i + (children.size() - i) / 2,
                children.size() - (children.size() - i) / 2 - i);
        nodes.add(m);
        result.add(m);
        break;
      } else {
        int[] n =
            indexNode(nodes, childrenStart + i, Integer.min(2 * order + 1, children.size() - i));
        nodes.add(n);
        result.add(n);
      }
    }

    if (result.size() <= 1) return result;
    else {
      return indexNodeHelper(result, nodes, order);
    }
  }

  /**
   * Creates a new index node
   *
   * @param nodes a master list of nodes that tracks the entire tree
   * @param pointer a pointer to the first child of the index node
   * @param numChildren the order of the tree to which the node belongs
   * @return an index node represented as an array of integers
   */
  private int[] indexNode(ArrayList<int[]> nodes, int pointer, int numChildren) {
    int[] node = new int[PAGE_SIZE / 4];
    node[0] = 1;
    node[1] = numChildren - 1;
    node[numChildren + 1] = pointer;
    // System.out.println(pointer);

    for (int i = 1; i < numChildren && i + pointer < nodes.size(); i++) {
      node[i + 1] = findSplitKey(i + pointer, nodes);
      node[i + numChildren + 1] = i + pointer;
    }

    return node;
  }

  /**
   * A helper function that traverses a subtree and finds the leftmost key it contains
   *
   * @param addr the address of the node on which to start
   * @param nodes an arraylist containing all the nodes in the tree
   * @return the leftmost key in the tree
   */
  private int findSplitKey(int addr, ArrayList<int[]> nodes) {
    while (nodes.get(addr)[0] == 1) {
      int[] n = nodes.get(addr);
      addr = n[n[1] + 2];
    }
    return nodes.get(addr)[2];
  }

  /**
   * Sequentially creates unclustered records from the table
   *
   * @param op A sorted operator
   * @param index The column on which the index is to be created
   * @return A new record in the <key,list> format
   */
  private ArrayList<Integer> makeRecord(Operator op, int index) {
    ArrayList<Integer> result = new ArrayList<>();
    if (nextTuple == null) return null;

    result.add(nextTuple.getElementAtIndex(index));
    result.add(0);

    while (nextTuple != null && nextTuple.getElementAtIndex(index) == result.get(0)) {

      int pid = nextTuple.getPID();
      int tid = nextTuple.getTID();
      boolean inserted = false;

      for (int i = 2; i < result.size(); i += 2) {
        if (pid < result.get(i)) {
          result.add(i, tid);
          result.add(i, pid);
          inserted = true;
          break;
        } else if (result.get(i).equals(pid)) {
          if (tid < result.get(i + 1)) {
            result.add(i, tid);
            result.add(i, pid);
            inserted = true;
            break;
          }
        }
      }
      if (!inserted) {
        result.add(pid);
        result.add(tid);
      }

      result.set(1, result.get(1) + 1);
      nextTuple = op.getNextTuple();
    }
    return result;
  }

  /**
   * Creates leaf nodes sequentially from a list of records
   *
   * @param iter an iterator on the list of records in the table
   * @param numRecords the number of records to be included in the leaf
   * @return a new leaf node represented as an array of integers
   */
  private int[] LeafNode(Iterator<ArrayList<Integer>> iter, int numRecords) {

    int[] node = new int[PAGE_SIZE / 4];
    node[0] = 0;
    node[1] = 0;
    int i = 2;

    for (int j = 0; iter.hasNext() && j < numRecords; j++) {
      Iterator<Integer> record = iter.next().iterator();
      while (record.hasNext()) {
        try {
          node[i] = record.next();
          i++;
        } catch (IndexOutOfBoundsException e) {
          return node;
        }
      }
      node[1]++;
    }
    return node;
  }

  /**
   * Writes a node to the file storing the tree index
   *
   * @param node The node to be written
   * @throws IOException
   */
  private void writeNode(int[] node) throws IOException {
    buffer.clear();
    IntBuffer temp = buffer.asIntBuffer();
    temp.put(node);
    // buffer.flip();
    fileChannel.write(buffer);
  }

  /**
   * Creates a header node containing the specified information
   *
   * @param root the address of the root node
   * @param leaves the number of leaves in the tree
   * @param order the order of the tree
   * @return a header node for the tree
   */
  private int[] headerNode(int root, int leaves, int order) {
    int[] node = new int[PAGE_SIZE / 4];
    node[0] = root;
    node[1] = leaves;
    node[2] = order;
    return node;
  }
}
