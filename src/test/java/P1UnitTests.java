import common.DBCatalog;
import common.QueryPlanBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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
import physical_operator.Operator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class P1UnitTests {
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private static String expectedPath;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {
    ClassLoader classLoader = P1UnitTests.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("samples/input")).getPath();
    DBCatalog.getInstance().setDataDirectory(path + "/db");
    expectedPath = Objects.requireNonNull(classLoader.getResource("samples/expected")).getPath();

    String queriesFile = Objects.requireNonNull(classLoader.getResource("samples/input/queries.sql")).getPath();
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

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query1"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(1));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query2"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery3() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(2));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query3"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery4() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(3));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query4"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery5() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(4));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query5"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery6() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(5));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query6"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery7() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(6));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query7"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery8() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(7));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query8"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery9() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(8));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query9"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery10() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(9));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query10"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery11() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(10));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query11"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery12() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(11));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query12"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery13() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(12));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query13"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery14() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(13));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query14"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testQuery15() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(14));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    plan.dump(new PrintStream(outputStream));

    byte[] output = outputStream.toByteArray();

    byte[] expected;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query15"));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}