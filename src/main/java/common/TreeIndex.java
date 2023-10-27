package common;

import net.sf.jsqlparser.schema.Column;
import physical_operator.Operator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;

public class TreeIndex {

    int order;
    int index;
    ArrayList<Tuple> leaves;
    File file;
    FileOutputStream fileOutputStream;
    FileChannel fileChannel;
    ByteBuffer buffer;
    Tuple nextTuple;
    Operator op;
    static final int PAGE_SIZE = 4096;

    public TreeIndex(String fileName, Operator op, int order, int indexElement) throws FileNotFoundException{
        this.file = new File(fileName);
        this.order = order;
        this.index = indexElement;
        this.leaves = new ArrayList<>();
        this.nextTuple = op.getNextTuple();
        this.op = op;

        try {
            fileOutputStream = new FileOutputStream(file);
            fileChannel = fileOutputStream.getChannel();
            this.buffer = ByteBuffer.allocate(PAGE_SIZE);
        } catch (FileNotFoundException e) {
            System.out.println("you shouldn't see this, ever");
        }


    }

    public int[] read(int page) {
        int[] result = new int[PAGE_SIZE/4];
        buffer.clear();
        buffer.asIntBuffer().get(result, 0, PAGE_SIZE/4);
        return result;
    }



    private void writePage(int[] page) throws IOException {
        buffer.clear();
        IntBuffer temp = buffer.asIntBuffer();
        temp.put(page);
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
