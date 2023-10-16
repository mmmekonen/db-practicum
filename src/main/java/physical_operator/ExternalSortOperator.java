package physical_operator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import common.DBCatalog;
import common.SortComparator;
import common.Tuple;
import common.TupleReader;
import common.TupleWriter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/** TODO */
public class ExternalSortOperator extends Operator {

    private Operator child;
    private ArrayList<Tuple> buffer;
    private int bufferSize;
    private Tuple currTuple;
    private File directory;
    private File sortedFile;
    private TupleReader readerSortedFile;
    public HashMap<Integer, Integer> indexOrders;

    /** TODO */
    public ExternalSortOperator(Operator child, List<Column> orderbyElements, int bufferPages) {
        super(null);

        this.child = child;

        indexOrders = new HashMap<>();
        createSortOrder(orderbyElements);

        // create directory to work in
        try {
            directory = new File(
                    Files.createTempDirectory(Paths.get(DBCatalog.getInstance().getSortDirectory()), null).toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.currTuple = child.getNextTuple();
        this.bufferSize = this.currTuple == null ? 0 : bufferPages * 4096 / (4 * currTuple.size());

        // sort step
        int numSortedRuns = sortBuffer();

        // merge step
        merge(bufferPages - 1, numSortedRuns);

        // create reader for getNextTuple
        try {
            this.readerSortedFile = new TupleReader(sortedFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // create output schema
        ArrayList<Column> schema = new ArrayList<>();
        for (int i = 0; i < child.outputSchema.size(); i++) {
            schema.add(child.outputSchema.get(indexOrders.get(i)));
        }
        // set outputSchema
        this.outputSchema = schema;
    }

    /** TODO */
    private void createSortOrder(List<Column> orderbyElements) {
        // gives the index in the tuple by order preference (ie. indexOrders[i] = the
        // index in child.outputSchema that is i-th in the final order)
        int position = 0;
        while (position < orderbyElements.size()) {
            Column column = orderbyElements.get(position);
            for (int i = 0; i < child.outputSchema.size(); i++) {
                Table t = child.outputSchema.get(i).getTable();
                String tableName2 = t.getAlias() != null ? t.getAlias().getName() : t.getName();
                if (child.outputSchema.get(i).getColumnName().equals(column.getColumnName())
                        && tableName2.equals(column.getTable().getName())) {
                    indexOrders.put(position, i);
                    position++;
                    break;
                }
            }
        }

        for (int i = 0; i < child.outputSchema.size(); i++) {
            if (!indexOrders.containsValue(i)) {
                indexOrders.put(position, i);
                position++;
            }
        }
    }

    /**
     * TODO
     * Populates the buffer array with tuples from the outer table
     */
    private boolean fillBuffer() {
        buffer = new ArrayList<Tuple>(bufferSize);
        boolean notEmpty = false;
        int i = 0;
        while (i < bufferSize && currTuple != null) {
            buffer.add(i, currTuple);
            currTuple = child.getNextTuple();
            notEmpty = true;
            i++;
        }
        return notEmpty;
    }

    /** TODO */
    private int sortBuffer() {
        int numSortedRuns = 0;
        while (fillBuffer()) {
            buffer.sort(new SortComparator(indexOrders));
            // write buffer
            try {
                TupleWriter tw = new TupleWriter(directory.toString() + "/" + numSortedRuns);
                for (Tuple t : buffer) {
                    tw.writeTuple(t);
                }
                tw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            numSortedRuns++;
        }
        return numSortedRuns;
    }

    /**
     * TOOD
     * 
     * @throws IOException
     */
    private void mergeHelper(List<TupleReader> runs, TupleWriter tw) throws IOException {
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (TupleReader tr : runs) {
            tuples.add(tr.readNextTuple());
        }
        SortComparator comparator = new SortComparator(indexOrders);
        Tuple smallest = Collections.min(tuples, comparator);
        while (smallest != null) {
            tw.writeTuple(smallest);
            int nextTR = tuples.indexOf(smallest);
            tuples.set(nextTR, runs.get(nextTR).readNextTuple());
            smallest = Collections.min(tuples, comparator);
        }
    }

    /** TODO */
    private void merge(int numMerges, int numSortedRuns) {
        int start = 0;
        int end = numSortedRuns;
        while (numSortedRuns > 1) {
            // grab numMerges runs to merge
            // numSortedRuns - numMerges to numSortedRuns-1
            ArrayList<TupleReader> trRuns = new ArrayList<>();
            for (int i = start; i < Math.min(start + numMerges, end); i++) {
                try {
                    trRuns.add(new TupleReader(directory.toString() + "/" + i));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                TupleWriter tw = new TupleWriter(directory.toString() + "/" + end);
                mergeHelper(trRuns, tw);
                for (TupleReader tr : trRuns) {
                    tr.close();
                }
                tw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            // decrease number of sorted runs & update start and end positions
            numSortedRuns -= numMerges - 1;
            start = Math.min(start + numMerges, end);
            end++;
        }
        sortedFile = new File(directory.toString() + "/" + start);
    }

    @Override
    public void reset() {
        try {
            readerSortedFile.close();
            readerSortedFile = new TupleReader(sortedFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (readerSortedFile == null) {
            return null;
        }
        try {
            Tuple t = readerSortedFile.readNextTuple();
            if (t == null) {
                readerSortedFile.close();
            }
            return t;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
