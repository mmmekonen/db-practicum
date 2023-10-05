import common.DBCatalog;
import common.QueryPlanBuilder;
import common.Tuple;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import operator.Operator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class P1FailedTests {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {
    ClassLoader classLoader = P1FailedTests.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("samples/input")).getPath();
    DBCatalog.getInstance().setDataDirectory(path + "/db");

    String queriesFile = Objects.requireNonNull(classLoader.getResource("samples/input/queries2.sql")).getPath();
    // for windows machine
    // if (queriesFile.contains(":")) {
    // queriesFile = queriesFile.substring(3);
    // }
    statements = CCJSqlParserUtil.parseStatements(Files.readString(Path.of(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();
  }

  @Test
  public void testQuery1() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(0));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 3;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(1, 101, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(1, 101, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(1, 101, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(1, 101, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(1, 101, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(1, 102, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(1, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(1, 102, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(1, 102, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(1, 102, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(1, 103, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(1, 103, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(1, 103, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(1, 103, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(1, 103, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(2, 101, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(2, 101, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(2, 101, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(2, 101, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(2, 101, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(3, 102, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(3, 102, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(3, 102, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(3, 102, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(3, 102, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(4, 104, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(4, 104, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(4, 104, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(4, 104, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(4, 104, 107, 2, 8)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(1));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {};

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery3() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(2));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 3;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(101, 2, 3, 110, 8, 1))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(102, 3, 4, 110, 8, 1))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(104, 104, 2, 110, 8, 1))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(103, 1, 1, 110, 8, 1))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(107, 2, 8, 110, 8, 1))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(109, 2, 100, 110, 8, 1))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(110, 3, 1, 110, 8, 1))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(110, 6, 1, 110, 8, 1))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(110, 7, 1, 110, 8, 1))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 101, 2, 3))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 102, 3, 4))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 104, 104, 2))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 103, 1, 1))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 107, 2, 8))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 109, 2, 100))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 110, 3, 1))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 110, 6, 1))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 110, 7, 1))),
        new Tuple(new ArrayList<>(List.of(110, 8, 1, 110, 8, 1))),
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }
}
