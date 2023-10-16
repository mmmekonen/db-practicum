import common.DBCatalog;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import physical_operator.ExternalSortOperator;
import physical_operator.InMemorySortOperator;
import physical_operator.Operator;
import physical_operator.ScanOperator;

public class ExternalSortTests {
  private static File outputDir;
  private static String tempDir = "src/test/resources/samples/temp";

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {
    ClassLoader classLoader = ExternalSortTests.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("samples")).getPath();
    DBCatalog.getInstance().setDataDirectory(path + "/input/db");
    DBCatalog.getInstance().setSortDirectory(tempDir);

    outputDir = new File("src/test/resources/samples/output");
    for (File file : (outputDir.listFiles()))
      file.delete(); // clean output directory
  }

  private void testHelper(Operator planInMemory, Operator planExternal, int testNum) {
    File outfileInMemory = new File(outputDir, "/testInMemory" + testNum);
    planInMemory.dump(outfileInMemory);

    File outfileExternal = new File(outputDir, "/testExternal" + testNum);
    planExternal.dump(outfileExternal);

    byte[] inMemory;
    byte[] external;
    try {
      inMemory = Files.readAllBytes(outfileInMemory.toPath());
      external = Files.readAllBytes(outfileExternal.toPath());
      Assertions.assertEquals(inMemory.length, external.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(external, inMemory), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
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

  @Test
  public void testQuery1() throws ExecutionControl.NotImplementedException {
    Operator scan = new ScanOperator("Boats", null);
    Operator planInMemory = new InMemorySortOperator(scan, new ArrayList<Column>(0));
    scan.reset();
    Operator planExternal = new ExternalSortOperator(scan, new ArrayList<Column>(0), 3);

    testHelper(planInMemory, planExternal, 1);
  }

  @Test
  public void testQuery2() throws ExecutionControl.NotImplementedException {
    Operator scan = new ScanOperator("Boats", null);
    Operator planInMemory = new InMemorySortOperator(scan, List.of(new Column(new Table(null, "Boats"), "D")));
    scan.reset();
    Operator planExternal = new ExternalSortOperator(scan, List.of(new Column(new Table(null, "Boats"), "D")), 3);

    testHelper(planInMemory, planExternal, 2);
  }
}
