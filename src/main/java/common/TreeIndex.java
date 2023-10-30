package common;

import net.sf.jsqlparser.schema.Column;
import physical_operator.Operator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class TreeIndex {

    int order;
    int index;
    File file;
    FileOutputStream fileOutputStream;
    FileChannel fileChannel;
    ByteBuffer buffer;
    Tuple nextTuple;
    ArrayList<Integer> nextRecord;
    Operator op;
    ArrayList<int[]> nodes;
    static final int PAGE_SIZE = 4096;

    public TreeIndex(String fileName, Operator op, int order, int indexElement) {
        this.file = new File(fileName);
        this.order = order;
        this.index = indexElement;
        this.nextTuple = op.getNextTuple();
        this.op = op;

        try {
            fileOutputStream = new FileOutputStream(file);
            fileChannel = fileOutputStream.getChannel();
            this.buffer = ByteBuffer.allocate(PAGE_SIZE);
        } catch (FileNotFoundException e) {
            System.out.println("you shouldn't see this, ever");
        }

        ArrayList<int[]> leaves = new ArrayList<>();

        while(nextTuple != null) {
            leaves.add(leafNode());
        }

        this.nodes = new ArrayList<>();
        nodes.add(new int[1]);
        nodes.addAll(leaves);
        indexNodeHelper(leaves);

        nodes.set(0, headerNode(nodes.size() - 1, leaves.size(), order));

        for(int i = 0; i < nodes.size(); i++) {
            try {
                writeNode(nodes.get(i));
            } catch (IOException e) {
                System.out.println("Tree loading failed");
            }
        }

    }

    public int[] readNode(int page) {
        int[] result = new int[PAGE_SIZE/4];
        buffer.clear();
        buffer.asIntBuffer().get(result, (page + 1)*PAGE_SIZE/4, PAGE_SIZE/4);
        return result;
    }

    private ArrayList<int[]> indexNodeHelper(ArrayList<int[]> children) {
        ArrayList<int[]> result = new ArrayList<>();

        for(int i = 0; i < children.size(); i += order) {
            int[] n = indexNode(children, i);
            nodes.add(n);
            result.add(n);
        }

        if (result.size() == 1) return result;
        else return indexNodeHelper(result);
    }

    private int[] indexNode(ArrayList<int[]> leaves, int pointer) {
        int[] node = new int[PAGE_SIZE/4];
        node[0] = 1;
        node[1] = 0;

        for(int i = 0; i < order; i++) {
            node[i + 2] = leaves.get(i + pointer)[3];
            node[i + order + 2] = i + pointer;
        }

        return node;
    }

    private ArrayList<Integer> makeRecord() {
        ArrayList<Integer> result = new ArrayList();
        result.add(nextTuple.getElementAtIndex(index));
        result.add(1);
        result.add(nextTuple.getPID());
        result.add(nextTuple.getTID());

        nextTuple = op.getNextTuple();
        if(nextTuple == null) return null;

        while(nextTuple != null && nextTuple.getElementAtIndex(index) == result.get(0)) {
            result.add(nextTuple.getPID());
            result.add(nextTuple.getTID());
            result.set(1, result.get(1) + 1);
            nextTuple = op.getNextTuple();
        }

        return result;
    }

    private int[] leafNode() {
        int[] node = new int[PAGE_SIZE/4];
        node[0] = 0;
        node[1] = 0;
        int i = 2;

        Iterator<Integer> record = nextRecord.iterator();
        while(nextRecord != null) {
            while (record.hasNext()) {
                try {
                    node[i] = record.next();
                    i++;
                } catch (IndexOutOfBoundsException e) {
                    return node;
                }
            }
            nextRecord = makeRecord();
        }
        return node;
    }

    private void writeNode(int[] node) throws IOException {
        buffer.clear();
        IntBuffer temp = buffer.asIntBuffer();
        temp.put(node);
        buffer.flip();
        fileChannel.write(buffer);
    }

    private int[] headerNode(int root, int leaves, int order) {
        int[] node = new int[PAGE_SIZE/4];
        node[0]= root;
        node[1] = leaves;
        node[2] = order;
        return node;
    }



}
