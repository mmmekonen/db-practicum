import common.DBCatalog;
import common.QueryPlanBuilder;
import common.TreeIndex;
import common.Tuple;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import jdk.jshell.spi.ExecutionControl;
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

public class P2FileTests {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private static String expectedPath = "src/test/resources/samples2/expected";
  private static File outputDir;
  private static String tempDir = "src/test/resources/samples2/temp";

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {

    ClassLoader classLoader = P2FileTests.class.getClassLoader();
    String path = "src/test/resources/samples2/input";

    DBCatalog.getInstance().setDataDirectory(path + "/db");
    DBCatalog.getInstance().setSortDirectory(tempDir);
    DBCatalog.getInstance().setIndexDirectory("src/test/resources/samples2/indexes");
    DBCatalog.getInstance().setIndexInfo();

    gatherStats(path, DBCatalog.getInstance());
    DBCatalog.getInstance().setStats(true);

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

  public static void gatherStats(String inputDir, DBCatalog db) {

    System.out.println("inputDir is " + inputDir);

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(inputDir + "/db/stats.txt")); ) {
      // DBCatalog db = DBCatalog.getInstance();
      Set<String> tableNames = db.getTableNames();

      System.out.println(tableNames);

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
        System.out.println("there are " + pid + " pages in " + table);
        System.out.println("there are " + len + " tuples in " + table);

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

  private void testPageEqualityHelper(String filename) {
    byte[] e;
    byte[] o;
    try {
      e = Files.readAllBytes(Path.of(expectedPath + "/" + filename));
      o = Files.readAllBytes(Path.of(outputDir + "/" + filename));
      Assertions.assertEquals(e.length, o.length, "Unexpected number of rows.");

      int expected[] = byteToIntArray(e);
      int output[] = byteToIntArray(o);

      for (int i = 0; i < expected.length; i += 1024) {
        int[] ex = Arrays.copyOfRange(expected, i, i + 1024);
        int[] op = Arrays.copyOfRange(output, i, i + 1024);

        Assertions.assertEquals(
            Arrays.toString(Arrays.copyOfRange(expected, i, i + 1024)),
            Arrays.toString(Arrays.copyOfRange(output, i, i + 1024)),
            "Outputs are not equal at page " + i / 1024);
      }
    } catch (IOException exception) {
      exception.printStackTrace();
      // System.out.println("hi");
    }
  }

  private int[] byteToIntArray(byte[] arr) {
    ByteBuffer bb = ByteBuffer.wrap(arr);
    int[] result = new int[arr.length / 4];

    for (int i = 0; i < result.length; i++) {
      result[i] = bb.getInt();
    }
    return result;
  }

  private void testFileHelper(Operator plan, int queryNum) {
    File outfile = new File(outputDir, "/query" + queryNum);
    plan.dump(outfile);

    int[] expected;
    int[] output;
    try {
      expected = byteToIntArray(Files.readAllBytes(Path.of(expectedPath + "/query" + queryNum)));
      output = byteToIntArray(Files.readAllBytes(outfile.toPath()));
      testPageEqualityHelper("query" + queryNum);
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("hi");
    }
  }

  /*
  @Test
  public void testQueryFile1() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(0));
    testFileHelper(plan, 1);
  }

  @Test
  public void testQueryFile2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(1));

    testFileHelper(plan, 2);
  }

  @Test
  public void testQueryFile3() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(2));
    testFileHelper(plan, 3);
  }

  @Test
  public void testQueryFile4() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(3));
    testFileHelper(plan, 4);
  }

  @Test
  public void testQueryFile5() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(4));
    testFileHelper(plan, 5);
  }

  @Test
  public void testQueryFile6() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(5));
    testFileHelper(plan, 6);
  }

  @Test
  public void testQueryFile7() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(6));
    testFileHelper(plan, 7);
  }
  */

  @Test
  public void testQueryFile11() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(10));
    testFileHelper(plan, 11);
  }

  /*
  @Test
  public void testQueryFile9() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(8));
    testFileHelper(plan, 9);
  }

  @Test
  public void testQueryFile10() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(9));
    testFileHelper(plan, 10);
  }

  @Test
  public void testQueryFile11() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(10));
    testFileHelper(plan, 11);
  }

  @Test
  public void testQueryFile12() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(11));
    testFileHelper(plan, 12);
  }
  */

  @Test
  public void testQueryFile13() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(12));
    testFileHelper(plan, 13);
  }

  @Test
  public void testQueryFile14() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(13));
    testFileHelper(plan, 14);
  }

  @Test
  public void testQueryFile15() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(14));
    testFileHelper(plan, 15);
  }
}
