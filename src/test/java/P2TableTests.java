import common.DBCatalog;
import common.QueryPlanBuilder;
import common.TreeIndex;
import common.Tuple;
import common.TupleReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import physical_operator.Operator;
import physical_operator.ScanOperator;

public class P2TableTests {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private static String expectedPath = "src/test/resources/samples2/expected";
  private static File outputDir;
  private static String tempDir = "src/test/resources/samples2/temp";

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {

    ClassLoader classLoader = P2TableTests.class.getClassLoader();
    String path = "src/test/resources/samples2/input";

    DBCatalog.getInstance().setDataDirectory(path + "/db");
    DBCatalog.getInstance().setSortDirectory(tempDir);
    DBCatalog.getInstance().setIndexDirectory("src/test/resources/samples2/input/db/indexes");
    DBCatalog.getInstance().setIndexInfo();

    gatherStats(path, DBCatalog.getInstance(), false);
    DBCatalog.getInstance().setStats(false);

    String queriesFile = "src/test/resources/samples2/input/queries.sql";

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Path.of(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();

    outputDir = new File("src/test/resources/samples2/output");
    for (File file : (outputDir.listFiles())) {
      file.delete(); // clean output directory
    }

    // Set up indexes
    ArrayList<String> tables = new ArrayList<>();
    DBCatalog db = DBCatalog.getInstance();
    tables.addAll(db.getIndexInfo().keySet());
    for (int i = 0; i < tables.size(); i++) {
      HashMap<String, ArrayList<Integer>> colmap = db.getIndexInfo().get(tables.get(i));
      for (String col : colmap.keySet()) {
        ArrayList<Integer> info = colmap.get(col);

        TreeIndex t =
            new TreeIndex(path, tables.get(i), col, db.findColumnIndex(tables.get(i), col), info);
      }
    }
  }

  @BeforeEach
  public void clearTempDir() {
    // clean temp directory before each query
    for (File file : (new File(tempDir).listFiles())) {
      if (file.isDirectory()) {
        for (File f : file.listFiles()) {
          f.delete();
        }
      }
      file.delete();
    }
  }

  public static void gatherStats(String inputDir, DBCatalog db, boolean verbose) {

    if (verbose) System.out.println("inputDir is " + inputDir);

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(inputDir + "/db/stats.txt")); ) {
      // DBCatalog db = DBCatalog.getInstance();
      Set<String> tableNames = db.getTableNames();

      if (verbose) System.out.println(tableNames);

      // store data
      HashMap<Integer, Integer> min = new HashMap<>();
      HashMap<Integer, Integer> max = new HashMap<>();
      int len = 0;

      // whether or not to write a newline in stats.txt
      boolean newline = false;
      for (String table : tableNames) {
        ArrayList<Column> columns = db.getTableSchema(table);
        Operator op = new ScanOperator(table, null);
        Tuple tuple;
        int pid = -1;
        while ((tuple = op.getNextTuple()) != null) {
          for (int i = 0; i < columns.size(); i++) {
            min.put(
                i,
                min.get(i) == null
                    ? tuple.getElementAtIndex(i)
                    : Math.min(min.get(i), tuple.getElementAtIndex(i)));
            max.put(
                i,
                max.get(i) == null
                    ? tuple.getElementAtIndex(i)
                    : Math.max(max.get(i), tuple.getElementAtIndex(i)));
          }
          len++;
          pid = tuple.getPID();
        }

        pid++;
        if (verbose) System.out.println("there are " + pid + " pages in " + table);
        if (verbose) System.out.println("there are " + len + " tuples in " + table);

        DBCatalog.getInstance().setNumPages(table, pid, len);

        // write to file
        if (newline) {
          bw.newLine();
        }
        bw.write(table + " " + len);
        for (int i = 0; i < columns.size(); i++) {
          bw.write(" " + columns.get(i).getColumnName() + "," + min.get(i) + "," + max.get(i));
        }
        newline = true;

        // clear hashmaps and length
        min.clear();
        max.clear();
        len = 0;
      }
    } catch (IOException e) {
      System.out.println("there was an error");
      e.printStackTrace();
    }
  }

  private String getPlanOutputs(int queryNum) {

    StringBuilder result = new StringBuilder();
    result.append("Logical Plan ").append(queryNum).append(":\n");
    result.append(queryPlanBuilder.logicalString(statementList.get(queryNum - 1)));
    result.append("\n\nPhysical Plan ").append(queryNum).append(":\n");
    result.append(queryPlanBuilder.physicalString(statementList.get(queryNum - 1)));
    return result.toString();
  }

  private void testTableHelper(Operator plan, int queryNum) {

    HashMap<Tuple, Integer> output = new HashMap<>();
    HashMap<Tuple, Integer> expected = new HashMap<>();

    // try {
    // // output logical plan
    // File logicalPlanFile = new File(outputDir, "query" + queryNum +
    // "_logicalplan");
    // FileWriter writer = new FileWriter(logicalPlanFile);
    // writer.write(queryPlanBuilder.logicalString(statementList.get(queryNum -
    // 1)));
    // writer.close();

    // // output physical plan
    // File physicalPlanFile = new File(outputDir, "query" + queryNum +
    // "_physicalplan");
    // writer = new FileWriter(physicalPlanFile);
    // writer.write(queryPlanBuilder.physicalString(statementList.get(queryNum -
    // 1)));
    // writer.close();
    // } catch (IOException e) {
    // e.printStackTrace();
    // }

    System.out.println(queryNum);
    Tuple next = plan.getNextTuple();
    while (next != null) {
      Integer temp = output.putIfAbsent(next, 1);
      if (temp != null) output.put(next, temp + 1);
      next = plan.getNextTuple();
    }

    try {
      TupleReader reader = new TupleReader(expectedPath + "/query" + queryNum);
      next = reader.readNextTuple();
      while (next != null) {
        Integer temp = expected.putIfAbsent(next, 1);
        if (temp != null) expected.put(next, temp + 1);
        next = reader.readNextTuple();
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    // System.out.println(getPlanOutputs(queryNum));

    Assertions.assertEquals(expected.keySet().size(), output.keySet().size());

    for (Tuple t : expected.keySet()) {
      Assertions.assertEquals(expected.get(t), output.get(t), "Key:" + t);
    }
  }

  @Test
  public void testQueryTable1() {
    int index = 1;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable2() {
    int index = 2;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable3() {
    int index = 3;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable4() {
    int index = 4;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable5() {
    int index = 5;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable6() {
    int index = 6;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable7() {
    int index = 7;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable8() {
    int index = 8;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable9() {
    int index = 9;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable10() {
    int index = 10;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable11() {
    int index = 11;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable12() {
    int index = 12;
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable13() {
    int index = 13;
    QueryPlanBuilder qpb = new QueryPlanBuilder();
    Operator plan = qpb.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable14() {
    int index = 14;
    QueryPlanBuilder qpb = new QueryPlanBuilder();
    Operator plan = qpb.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }

  @Test
  public void testQueryTable15() {
    int index = 15;

    QueryPlanBuilder qpb = new QueryPlanBuilder();
    Operator plan = qpb.buildPlan(statementList.get(index - 1));
    testTableHelper(plan, index);
  }
}
