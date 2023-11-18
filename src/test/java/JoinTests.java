import common.DBCatalog;
import common.QueryPlanBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import physical_operator.Operator;

public class JoinTests {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private static File outputDir;
  private static String tempDir = "src/test/resources/samples/temp";

  int TNLJ = 0;
  int BNLJ = 1;
  int SMJ = 2;
  int defaultJoinBuffer = 5;
  int defaultSort = 0;
  int defaultSortBuffer = 3;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {

    ClassLoader classLoader = JoinTests.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("samples/input")).getPath();

    DBCatalog.getInstance().setDataDirectory(path + "/db");
    DBCatalog.getInstance().setSortDirectory(tempDir);

    String queriesFile = Objects.requireNonNull(classLoader.getResource("samples/input/queries2.sql")).getPath();
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

  private void testHelper(Operator tnlj, Operator bnlj, Operator smj, int queryNum) {
    File tnljFile = new File(outputDir, "/queryTNLJ" + queryNum);
    File bnljFile = new File(outputDir, "/queryBNLJ" + queryNum);
    File smjFile = new File(outputDir, "/querySMJ" + queryNum);
    tnlj.dump(tnljFile);
    bnlj.dump(bnljFile);
    smj.dump(smjFile);

    byte[] tnljOutput;
    byte[] bnljOutput;
    byte[] smjOutput;
    try {
      tnljOutput = Files.readAllBytes(tnljFile.toPath());
      bnljOutput = Files.readAllBytes(bnljFile.toPath());
      smjOutput = Files.readAllBytes(smjFile.toPath());

      Assertions.assertEquals(tnljOutput.length, bnljOutput.length, "Unexpected number of rows, BNLJ.");
      Assertions.assertTrue(Arrays.equals(tnljOutput, bnljOutput), "Outputs are not equal, BNLJ.");

      Assertions.assertEquals(tnljOutput.length, smjOutput.length, "Unexpected number of rows, SMJ.");
      Assertions.assertTrue(Arrays.equals(tnljOutput, smjOutput), "Outputs are not equal, SMJ.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery1() throws ExecutionControl.NotImplementedException {
    Operator tnlj = queryPlanBuilder.buildPlan(statementList.get(0), TNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator bnlj = queryPlanBuilder.buildPlan(statementList.get(0), BNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator smj = queryPlanBuilder.buildPlan(statementList.get(0), SMJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);

    testHelper(tnlj, bnlj, smj, 1);
  }

  @Test
  public void testQuery2() throws ExecutionControl.NotImplementedException {
    Operator tnlj = queryPlanBuilder.buildPlan(statementList.get(1), TNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator bnlj = queryPlanBuilder.buildPlan(statementList.get(1), BNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator smj = queryPlanBuilder.buildPlan(statementList.get(1), SMJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);

    testHelper(tnlj, bnlj, smj, 2);
  }

  @Test
  public void testQuery3() throws ExecutionControl.NotImplementedException {
    Operator tnlj = queryPlanBuilder.buildPlan(statementList.get(2), TNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator bnlj = queryPlanBuilder.buildPlan(statementList.get(2), BNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator smj = queryPlanBuilder.buildPlan(statementList.get(2), SMJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);

    testHelper(tnlj, bnlj, smj, 3);
  }

  // @Test
  // public void testQuery4() throws ExecutionControl.NotImplementedException {
  // Operator tnlj = queryPlanBuilder.buildPlan(statementList.get(3), TNLJ,
  // defaultJoinBuffer, defaultSort,
  // defaultSortBuffer);
  // Operator bnlj = queryPlanBuilder.buildPlan(statementList.get(3), BNLJ,
  // defaultJoinBuffer, defaultSort,
  // defaultSortBuffer);
  // Operator smj = queryPlanBuilder.buildPlan(statementList.get(3), SMJ,
  // defaultJoinBuffer, defaultSort,
  // defaultSortBuffer);

  // testHelper(tnlj, bnlj, smj, 4);
  // }

  @Test
  public void testQuery5() throws ExecutionControl.NotImplementedException {
    Operator tnlj = queryPlanBuilder.buildPlan(statementList.get(4), TNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator bnlj = queryPlanBuilder.buildPlan(statementList.get(4), BNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator smj = queryPlanBuilder.buildPlan(statementList.get(4), SMJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);

    testHelper(tnlj, bnlj, smj, 5);
  }

  @Test
  public void testQuery6() throws ExecutionControl.NotImplementedException {
    Operator tnlj = queryPlanBuilder.buildPlan(statementList.get(5), TNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator bnlj = queryPlanBuilder.buildPlan(statementList.get(5), BNLJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);
    Operator smj = queryPlanBuilder.buildPlan(statementList.get(5), SMJ, defaultJoinBuffer, defaultSort,
        defaultSortBuffer);

    testHelper(tnlj, bnlj, smj, 6);
  }

  // @Test
  // public void testQuery7() throws ExecutionControl.NotImplementedException {
  // Operator tnlj = queryPlanBuilder.buildPlan(statementList.get(6), TNLJ,
  // defaultJoinBuffer, defaultSort,
  // defaultSortBuffer);
  // Operator bnlj = queryPlanBuilder.buildPlan(statementList.get(6), BNLJ,
  // defaultJoinBuffer, defaultSort,
  // defaultSortBuffer);
  // Operator smj = queryPlanBuilder.buildPlan(statementList.get(6), SMJ,
  // defaultJoinBuffer, defaultSort,
  // defaultSortBuffer);

  // testHelper(tnlj, bnlj, smj, 7);
  // }

  // @Test
  // public void testQuery8() throws ExecutionControl.NotImplementedException {
  // Operator tnlj = queryPlanBuilder.buildPlan(statementList.get(7), TNLJ,
  // defaultJoinBuffer, defaultSort,
  // defaultSortBuffer);
  // Operator bnlj = queryPlanBuilder.buildPlan(statementList.get(7), BNLJ,
  // defaultJoinBuffer, defaultSort,
  // defaultSortBuffer);
  // Operator smj = queryPlanBuilder.buildPlan(statementList.get(7), SMJ,
  // defaultJoinBuffer, defaultSort,
  // defaultSortBuffer);

  // testHelper(tnlj, bnlj, smj, 8);
  // }
}