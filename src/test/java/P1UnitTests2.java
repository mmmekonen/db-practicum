import common.DBCatalog;
import common.QueryPlanBuilder;
import common.Tuple;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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

public class P1UnitTests2 {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {
    ClassLoader classLoader = P1UnitTests2.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("samples/input")).getPath();
    DBCatalog.getInstance().setDataDirectory(path + "/db");

    String queriesFile =
        Objects.requireNonNull(classLoader.getResource("samples/input/queries2.sql")).getPath();
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

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(List.of(4, 100, 50))),
          new Tuple(new ArrayList<>(List.of(5, 100, 500))),
          new Tuple(new ArrayList<>(List.of(6, 300, 400)))
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

    int expectedSize = 3;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(List.of(3, 100, 105))),
          new Tuple(new ArrayList<>(List.of(5, 100, 500))),
          new Tuple(new ArrayList<>(List.of(6, 300, 400)))
        };

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

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(List.of(4, 100))),
          new Tuple(new ArrayList<>(List.of(5, 100))),
          new Tuple(new ArrayList<>(List.of(6, 300)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery4() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(3));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(List.of(1, 200))),
          new Tuple(new ArrayList<>(List.of(2, 200))),
          new Tuple(new ArrayList<>(List.of(3, 100))),
          new Tuple(new ArrayList<>(List.of(4, 100))),
          new Tuple(new ArrayList<>(List.of(5, 100))),
          new Tuple(new ArrayList<>(List.of(6, 300)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery5() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(4));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(List.of(50, 200, 1))),
          new Tuple(new ArrayList<>(List.of(200, 200, 2))),
          new Tuple(new ArrayList<>(List.of(105, 100, 3))),
          new Tuple(new ArrayList<>(List.of(50, 100, 4))),
          new Tuple(new ArrayList<>(List.of(500, 100, 5))),
          new Tuple(new ArrayList<>(List.of(400, 300, 6)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery6() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(5));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 3;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(List.of(4, 100, 50))),
          new Tuple(new ArrayList<>(List.of(5, 100, 500))),
          new Tuple(new ArrayList<>(List.of(6, 300, 400)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery7() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(6));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 3;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(List.of(4, 100))),
          new Tuple(new ArrayList<>(List.of(5, 100))),
          new Tuple(new ArrayList<>(List.of(6, 300)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery8() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(7));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(List.of(1, 200))),
          new Tuple(new ArrayList<>(List.of(2, 200))),
          new Tuple(new ArrayList<>(List.of(3, 100))),
          new Tuple(new ArrayList<>(List.of(4, 100))),
          new Tuple(new ArrayList<>(List.of(5, 100))),
          new Tuple(new ArrayList<>(List.of(6, 300)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery9() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(8));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(List.of(50, 200, 1))),
          new Tuple(new ArrayList<>(List.of(200, 200, 2))),
          new Tuple(new ArrayList<>(List.of(105, 100, 3))),
          new Tuple(new ArrayList<>(List.of(50, 100, 4))),
          new Tuple(new ArrayList<>(List.of(500, 100, 5))),
          new Tuple(new ArrayList<>(List.of(400, 300, 6)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery10() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(9));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 4;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(1))),
          new Tuple(new ArrayList<>(Arrays.asList(2))),
          new Tuple(new ArrayList<>(Arrays.asList(3))),
          new Tuple(new ArrayList<>(Arrays.asList(4)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery11() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(10));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(3, 100, 105))),
          new Tuple(new ArrayList<>(Arrays.asList(4, 100, 50))),
          new Tuple(new ArrayList<>(Arrays.asList(5, 100, 500))),
          new Tuple(new ArrayList<>(Arrays.asList(1, 200, 50))),
          new Tuple(new ArrayList<>(Arrays.asList(2, 200, 200))),
          new Tuple(new ArrayList<>(Arrays.asList(6, 300, 400)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery12() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(11));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(1, 200, 50, 1, 101))),
          new Tuple(new ArrayList<>(Arrays.asList(1, 200, 50, 1, 102))),
          new Tuple(new ArrayList<>(Arrays.asList(1, 200, 50, 1, 103))),
          new Tuple(new ArrayList<>(Arrays.asList(2, 200, 200, 2, 101))),
          new Tuple(new ArrayList<>(Arrays.asList(3, 100, 105, 3, 102))),
          new Tuple(new ArrayList<>(Arrays.asList(4, 100, 50, 4, 104)))
        };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery13() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(12));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 11;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    Tuple[] expectedTuples = new Tuple[] {
        new Tuple(new ArrayList<>(Arrays.asList(1, 1, 101, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(1, 1, 102, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(1, 1, 103, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(1, 1, 104, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(1, 1, 107, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(3, 3, 107, 105))),
        new Tuple(new ArrayList<>(Arrays.asList(4, 4, 101, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(4, 4, 102, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(4, 4, 103, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(4, 4, 104, 50))),
        new Tuple(new ArrayList<>(Arrays.asList(4, 4, 107, 50)))
    };

    for (int i = 0; i < expectedSize; i++) {
      Tuple expectedTuple = expectedTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }
}
