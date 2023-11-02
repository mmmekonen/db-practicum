package common;

import net.sf.jsqlparser.schema.Column;
import physical_operator.InMemorySortOperator;
import physical_operator.Operator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TreeIndex {

    File file;
    FileOutputStream fileOutputStream;
    FileChannel fileChannel;
    ByteBuffer buffer;
    Tuple nextTuple;
    ArrayList<Integer> nextRecord;
    static final int PAGE_SIZE = 4096;
    int curPage;

    /**
     * Class to create and read a B+ tree index
     * 
     * @param fileName     The file to which the tree will be written
     * @param op           A sorted operator on the table to be indexed
     * @param order        The order of the tree index
     * @param indexElement The element on which the table will be indexed
     */
    public TreeIndex(String fileName, InMemorySortOperator op, int order, int indexElement, boolean clustered) {
        this.file = new File(fileName);
        this.nextTuple = op.getNextTuple();

        try {
            fileOutputStream = new FileOutputStream(file);
            fileChannel = fileOutputStream.getChannel();
            this.buffer = ByteBuffer.allocate(PAGE_SIZE);
            file.createNewFile();
        } catch (Exception e) {
            System.out.println("you shouldn't see this, ever");
        }

        ArrayList<int[]> leaves = new ArrayList<>();

        while (nextTuple != null)
            leaves.add(LeafNode(op, indexElement, clustered));

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
            fileOutputStream = new FileOutputStream(file);
            fileChannel = fileOutputStream.getChannel();
            this.buffer = ByteBuffer.allocate(PAGE_SIZE);
        } catch (FileNotFoundException e) {
            System.out.println("you shouldn't see this, ever");
        }

    }

    /**
     * Reads the node stored in the specified page
     * 
     * @param page the page to be read
     * @return The data in a page, represented as an array of integers
     */
    public int[] readNode(int page) {
        int[] result = new int[PAGE_SIZE / 4];
        buffer.clear();
        buffer.asIntBuffer().get(result, (page + 1) * PAGE_SIZE / 4, PAGE_SIZE / 4);

        curPage = page;
        return result;
    }

    public int[] deserialize(int page, Integer traverseKey) {
        readNode(page);
        if (buffer.getInt(0) == 1) {
            return deserializeIndex(page, traverseKey);
        } else {
            return deserializeLeaf(page);
        }
    }

    // PROBABLY NOT RIGHT
    public int[] getNextLeaf() {
        return readNode(curPage + 1);
    }

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

        int index = 0;
        while (index < numKeys && keys[index] < traverseKey) {
            index++;
        }

        return deserialize(children[index], traverseKey);
    }

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

        int[] result = new int[pos + numDataEntries * 2];
        result[0] = page;
        result[1] = numDataEntries;
        result[2] = 0;

        int index = 3;
        for (Integer key : keys) {
            result[index++] = key;
        }

        for (List<Integer> entry : entries) {
            for (Integer rid : entry) {
                result[index++] = rid;
            }
        }

        return result;
    }

    public ArrayList<ArrayList<Integer>> getDataEntries() {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        int numDataEntries = buffer.getInt(4);
        int pos = 8;

        for (int i = 0; i < numDataEntries; i++) {
            int key = buffer.getInt(pos);
            pos += 4;

            int numRids = buffer.getInt(pos);
            pos += 4;

            ArrayList<Integer> rids = new ArrayList<>();
            rids.add(key);
            for (int j = 0; j < numRids; j++) {
                int pageNum = buffer.getInt(pos);
                pos += 4;
                int tupleNum = buffer.getInt(pos);
                pos += 4;

                rids.add(pageNum);
                rids.add(tupleNum);
            }

            result.add(rids);
        }

        return result;
    }

    /**
     * Helper function to recursively create the index nodes of the table
     * 
     * @param children The layer of nodes beneath the ones to be created
     * @param nodes    A master list of nodes that tracks the entire tree
     * @param order    The order of the tree
     * @return A newly created layer of nodes, represented as an arraylist
     */

    private ArrayList<int[]> indexNodeHelper(ArrayList<int[]> children, ArrayList<int[]> nodes, int order) {
        ArrayList<int[]> result = new ArrayList<>();
        int childrenStart = nodes.size() - children.size();

        for (int i = order; i < children.size(); i += order) {
            int[] n = indexNode(children, childrenStart, order);
            nodes.add(n);
            result.add(n);
        }

        if (result.size() == 1)
            return result;
        else
            return indexNodeHelper(result, nodes, order);
    }

    /*
     * private void indexNodeHelper(ArrayList<int[]> nodes, int start, int order) {
     * int nodesAdded = 0;
     * 
     * for(int i = order; i < children.size(); i += order) {
     * int[] n = indexNode(children, start, order);
     * nodes.add(n);
     * nodesAdded++;
     * }
     * 
     * if (nodesAdded == 1) return;
     * else indexNodeHelper(nodes, start + resultSize, order);
     * }
     */

    /**
     * Creates a new index node
     * 
     * @param nodes   a master list of nodes that tracks the entire tree
     * @param pointer a pointer to the first child of the index node
     * @param order   the order of the tree to which the node belongs
     * @return an index node represented as an array of integers
     */
    private int[] indexNode(ArrayList<int[]> nodes, int pointer, int order) {
        int[] node = new int[PAGE_SIZE / 4];
        node[0] = 1;
        node[1] = 0;

        for (int i = 0; i < order; i++) {
            node[i + 2] = nodes.get(i + pointer)[3];
            node[i + order + 2] = i + pointer;
        }

        return node;
    }

    /**
     * Sequentially creates unclustered records from the table
     * 
     * @param op    A sorted operator
     * @param index The index to be created
     * @return A new record
     */
    private ArrayList<Integer> makeRecord(Operator op, int index, boolean clustered) {
        ArrayList<Integer> result = new ArrayList();
        result.add(nextTuple.getElementAtIndex(index));
        result.add(1);
        result.add(nextTuple.getPID());
        result.add(nextTuple.getTID());

        nextTuple = op.getNextTuple();
        if (nextTuple == null)
            return null;

        while (nextTuple != null && nextTuple.getElementAtIndex(index) == result.get(0)) {
            if (clustered) {
                ArrayList temp = nextTuple.getAllElements();
                temp.remove(index);
                result.addAll(temp);
            } else {
                result.add(nextTuple.getPID());
                result.add(nextTuple.getTID());
            }
            result.set(1, result.get(1) + 1);
            nextTuple = op.getNextTuple();
        }

        return result;
    }

    /**
     * Creates leaf nodes sequentially from the table
     * 
     * @param op    a sorted operator
     * @param index the index to be created
     * @return a new leaf node represented as an array of integers
     */
    private int[] LeafNode(Operator op, int index, boolean clustered) {
        int[] node = new int[PAGE_SIZE / 4];
        node[0] = 0;
        node[1] = 0;
        int i = 2;

        Iterator<Integer> record = nextRecord.iterator();
        while (nextRecord != null) {
            while (record.hasNext()) {
                try {
                    node[i] = record.next();
                    i++;
                } catch (IndexOutOfBoundsException e) {
                    return node;
                }
            }
            nextRecord = makeRecord(op, index, clustered);
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
        buffer.flip();
        fileChannel.write(buffer);
    }

    /**
     * Creates a header node containing the specified information
     * 
     * @param root   the address of the root node
     * @param leaves the number of leaves in the tree
     * @param order  the order of the tree
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
