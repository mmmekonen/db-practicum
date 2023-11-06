import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// To use these tests, put the output from running the jar file into cmdline_outputs

public class P2ManualTests {
  private static String expectedPath;
  private static File outputDir;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {
    expectedPath = "src/test/resources/samples/expected";
    outputDir = new File("src/test/resources/cmdline_outputs");
  }

  private void testHelper(int queryNum) {
    File outfile = new File(outputDir, "/query" + queryNum);

    byte[] expected;
    byte[] output;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/query" + queryNum));
      output = Files.readAllBytes(outfile.toPath());
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      System.out.println(e);
    }
  }

  @Test
  public void testQuery1() throws ExecutionControl.NotImplementedException {
    testHelper(1);
  }

  @Test
  public void testQuery2() throws ExecutionControl.NotImplementedException {
    testHelper(2);
  }

  @Test
  public void testQuery3() throws ExecutionControl.NotImplementedException {
    testHelper(3);
  }

  @Test
  public void testQuery4() throws ExecutionControl.NotImplementedException {
    testHelper(4);
  }

  @Test
  public void testQuery5() throws ExecutionControl.NotImplementedException {
    testHelper(5);
  }

  @Test
  public void testQuery6() throws ExecutionControl.NotImplementedException {
    testHelper(6);
  }

  @Test
  public void testQuery7() throws ExecutionControl.NotImplementedException {
    testHelper(7);
  }

  @Test
  public void testQuery8() throws ExecutionControl.NotImplementedException {
    testHelper(8);
  }

  @Test
  public void testQuery9() throws ExecutionControl.NotImplementedException {
    testHelper(9);
  }

  @Test
  public void testQuery10() throws ExecutionControl.NotImplementedException {
    testHelper(10);
  }

  @Test
  public void testQuery11() throws ExecutionControl.NotImplementedException {
    testHelper(11);
  }

  @Test
  public void testQuery12() throws ExecutionControl.NotImplementedException {
    testHelper(12);
  }

  @Test
  public void testQuery13() throws ExecutionControl.NotImplementedException {
    testHelper(13);
  }

  @Test
  public void testQuery14() throws ExecutionControl.NotImplementedException {
    testHelper(14);
  }

  @Test
  public void testQuery15() throws ExecutionControl.NotImplementedException {
    testHelper(15);
  }
}
