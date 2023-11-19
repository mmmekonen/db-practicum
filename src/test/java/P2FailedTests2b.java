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

public class P2FailedTests2b {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private static String expectedPath;
  private static File outputDir;
  private static String tempDir = "src/test/resources/failed_tests/2b/temp";

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {

    ClassLoader classLoader = P2FailedTests2b.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("failed_tests/2b/input")).getPath();

    DBCatalog.getInstance().setDataDirectory(path + "/db");
    DBCatalog.getInstance().setSortDirectory(tempDir);

    expectedPath = "src/test/resources/failed_tests/2b/expected";

    String queriesFile = Objects.requireNonNull(classLoader.getResource("failed_tests/2b/input/queries.sql")).getPath();
    // for windows machine
    if (queriesFile.contains(":")) {
      queriesFile = queriesFile.substring(3);
    }

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Path.of(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();

    outputDir = new File("src/test/resources/failed_tests/2b/output");
    for (File file : (outputDir.listFiles()))
      file.delete(); // clean output directory
  }

  private void testHelper(Operator plan, int queryNum) {
    File outfile = new File(outputDir, "/query" + queryNum);
    plan.dump(outfile);

    byte[] expected;
    byte[] output;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query" + queryNum));
      output = Files.readAllBytes(outfile.toPath());
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
      /*
       * TupleReader e = new TupleReader(expectedPath + "/query" + queryNum);
       * TupleReader o = new TupleReader(outfile.toPath().toString());
       * Tuple t = e.readNextTuple();
       * HashSet<Tuple> hs = new HashSet();
       * System.out.println(1);
       * while (t != null) {
       * hs.add(t);
       * t = e.readNextTuple();
       * }
       * e.close();
       * t = o.readNextTuple();
       * System.out.println(2);
       * while(t != null) {
       * Assertions.assertTrue(hs.contains(t));
       * t = o.readNextTuple();
       * }
       * o.close();
       */
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("hi");
    }
  }

  @Test
  public void testQuery1() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(0));

    testHelper(plan, 1);
  }

  @Test
  public void testQuery2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(1));

    testHelper(plan, 2);
  }

  @Test
  public void testQuery3() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(2));
    testHelper(plan, 3);
  }

  @Test
  public void testQuery4() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(3));
    testHelper(plan, 4);
  }

  @Test
  public void testQuery5() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(4));
    testHelper(plan, 5);
  }
}
