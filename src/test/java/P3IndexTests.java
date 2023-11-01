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

public class P3IndexTests {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private static String expectedPath;
  private static File outputDir;
  private static String tempDir = "src/test/resources/samples/temp";

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {

    ClassLoader classLoader = P3IndexTests.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("samples/input")).getPath();

    DBCatalog.getInstance().setDataDirectory(path + "/db");
    DBCatalog.getInstance().setSortDirectory(tempDir);
    DBCatalog.getInstance().setIndexInfo();

    expectedPath = "src/test/resources/samples/expected_indexes";
    outputDir = new File("src/test/resources/samples/db/indexes");
    for (File file : (outputDir.listFiles()))
      file.delete(); // clean output directory

    // TODO: make indexes
  }

  private void testHelper(String filename) {
    byte[] expected;
    byte[] output;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/" + filename));
      output = Files.readAllBytes(Path.of(outputDir + "/" + filename));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("hi");
    }
  }

  @Test
  public void testIndex1() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(0));

    testHelper("Boats.E");
  }

  @Test
  public void testIndex2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(1));

    testHelper("Sailors.A");
  }
}
