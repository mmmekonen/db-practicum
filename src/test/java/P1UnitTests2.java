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

    Tuple[] expectedTuples = new Tuple[] {
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
}