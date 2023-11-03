import common.DBCatalog;
import common.QueryPlanBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import common.Tuple;
import common.TupleReader;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import physical_operator.Operator;

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

    DBCatalog.getInstance().setDataDirectory(path + "/db");
    DBCatalog.getInstance().setSortDirectory(tempDir);
    DBCatalog.getInstance().setIndexDirectory("src/test/resources/samples/expected_indexes");
    DBCatalog.getInstance().setIndexInfo();

    String queriesFile = Objects.requireNonNull(classLoader.getResource("samples/input/queries.sql")).getPath();
    // for windows machine
    if (queriesFile.contains(":")) {
      queriesFile = queriesFile.substring(3);
    }

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Path.of(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();

    outputDir = new File("src/test/resources/samples/output");
    for (File file : (outputDir.listFiles()))
      file.delete(); // clean output directory
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

  @Test
  public void testQuery1() throws ExecutionControl.NotImplementedException {
    DBCatalog.getInstance().setUseIndex(true);
    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(0));

    DBCatalog.getInstance().setUseIndex(false);
    Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(0));

    testHelper(planIndex, planNoIndex, 1);
  }

  @Test
  public void testQuery2() throws ExecutionControl.NotImplementedException {
    DBCatalog.getInstance().setUseIndex(true);
    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(1));

    DBCatalog.getInstance().setUseIndex(false);
    Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(1));

    testHelper(planIndex, planNoIndex, 2);
  }

  @Test
  public void testQuery3() throws ExecutionControl.NotImplementedException {
    DBCatalog.getInstance().setUseIndex(true);
    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(2));

    DBCatalog.getInstance().setUseIndex(false);
    Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(2));

    testHelper(planIndex, planNoIndex, 3);
  }

  @Test
  public void testQuery4() throws ExecutionControl.NotImplementedException {
    DBCatalog.getInstance().setUseIndex(true);
    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(3));

    DBCatalog.getInstance().setUseIndex(false);
    Operator planNoIndex = queryPlanBuilder.buildPlan(statementList.get(3));

    testHelper(planIndex, planNoIndex, 4);
  }

  @Test
  public void testQuery5() throws ExecutionControl.NotImplementedException {
    DBCatalog.getInstance().setUseIndex(true);
    Operator planIndex = queryPlanBuilder.buildPlan(statementList.get(4));

    DBCatalog.getInstance().setUseIndex(false);
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
