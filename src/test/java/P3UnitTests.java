import common.DBCatalog;
import common.QueryPlanBuilder;
import common.Tuple;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
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

public class P3UnitTests {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private static File outputDir;
  private static String tempDir = "src/test/resources/samples/temp";

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {

    ClassLoader classLoader = P3UnitTests.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("samples/input")).getPath();

    System.out.println("path is " + path);

    DBCatalog.getInstance().setDataDirectory(path + "/db");
    DBCatalog.getInstance().setSortDirectory(tempDir);
    DBCatalog.getInstance().setIndexDirectory("src/test/resources/samples/expected_indexes");
    DBCatalog.getInstance().setIndexInfo();

    gatherStats(path, DBCatalog.getInstance());
    DBCatalog.getInstance().setStats(false);

    String queriesFile =
        Objects.requireNonNull(classLoader.getResource("samples/input/queries.sql")).getPath();
    // for windows machine
    if (queriesFile.contains(":")) {
      queriesFile = queriesFile.substring(3);
    }

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Path.of(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();

    outputDir = new File("src/test/resources/samples/output");
    for (File file : (outputDir.listFiles())) file.delete(); // clean output directory
  }

  private void testHelper(Operator planIndex, Operator planNoIndex, int queryNum) {
    File outfileIndex = new File(outputDir, "/query_Index" + queryNum);
    planIndex.dump(outfileIndex);
    File outfileNoIndex = new File(outputDir, "/query_NoIndex" + queryNum);
    planNoIndex.dump(outfileNoIndex);

    byte[] noIndex;
    byte[] withindex;
    try {
      noIndex = Files.readAllBytes(outfileNoIndex.toPath());
      withindex = Files.readAllBytes(outfileIndex.toPath());
      Assertions.assertEquals(noIndex.length, withindex.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(withindex, noIndex), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("hi");
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

  @Test
  public void testQuery1() throws ExecutionControl.NotImplementedException {
    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(0));

    Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(0));

    testHelper(planIndex, planNoIndex, 1);
  }

  @Test
  public void testQuery2() throws ExecutionControl.NotImplementedException {

    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(1));

    Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(1));

    testHelper(planIndex, planNoIndex, 2);
  }

  @Test
  public void testQuery3() throws ExecutionControl.NotImplementedException {
    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(2));

    Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(2));

    testHelper(planIndex, planNoIndex, 3);
  }

  @Test
  public void testQuery4() throws ExecutionControl.NotImplementedException {
    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(3));

    Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(3));

    testHelper(planIndex, planNoIndex, 4);
  }

  @Test
  public void testQuery5() throws ExecutionControl.NotImplementedException {
    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(4));

    Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(4));

    testHelper(planIndex, planNoIndex, 5);
  }

  // @Test
  // public void testQuery6() throws ExecutionControl.NotImplementedException {
  // DBCatalog.getInstance().setUseIndex(true);
  // Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(5));

  // DBCatalog.getInstance().setUseIndex(false);
  // Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(5));

  // testHelper(planIndex, planNoIndex, 6);
  // }
}
