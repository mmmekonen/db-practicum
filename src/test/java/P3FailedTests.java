import common.DBCatalog;
import common.TreeIndex;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import physical_operator.InMemorySortOperator;
import physical_operator.ScanOperator;

public class P3FailedTests {
  private static String expectedPath;
  private static String outputPath;
  private static String tempDir = "src/test/resources/failed_tests/tempSort";

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {

    ClassLoader classLoader = P3FailedTests.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("failed_tests/input")).getPath();

    DBCatalog db = DBCatalog.getInstance();
    db.setDataDirectory(path + "/db");
    db.setSortDirectory(tempDir);
    db.setIndexInfo();

    expectedPath = "src/test/resources/failed_tests/expected_indexes";
    outputPath = "src/test/resources/failed_tests/input/db/indexes";

    // make indexes
    ArrayList<String> tables = new ArrayList<>();
    tables.addAll(db.getIndexInfo().keySet());

    for (int i = 0; i < tables.size(); i++) {

      ArrayList<String> info = db.getIndexInfo().get(tables.get(i));

      TreeIndex t = new TreeIndex(
          "src/test/resources/failed_tests/input", tables.get(i), db.findColumnIndex(tables.get(i), info.get(0)), info);
    }

    // ArrayList<String> tables = new ArrayList<>();
    // tables.addAll(db.getIndexInfo().keySet());

    // for (int i = 0; i < tables.size(); i++) {

    // ArrayList<String> info = db.getIndexInfo().get(tables.get(i));

    // ScanOperator base = new ScanOperator(tables.get(i), null);
    // ArrayList<Column> temp = new ArrayList<>();
    // temp.add(new Column(new Table(null, tables.get(i)), info.get(0)));
    // InMemorySortOperator op = new InMemorySortOperator(base, temp);

    // TreeIndex t = new TreeIndex(
    // "src/test/resources/failed_tests/input",
    // tables.get(i),
    // db.findColumnIndex(tables.get(i), info.get(0)),
    // info);
    // }
  }

  private void testSizeHelper(String filename) {
    byte[] expected;
    byte[] output;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/" + filename));
      output = Files.readAllBytes(Path.of(outputPath + "/" + filename));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
    } catch (IOException e) {
      e.printStackTrace();
      // System.out.println("hi");
    }
  }

  private void testFullEqualityHelper(String filename) {
    byte[] expected;
    byte[] output;
    try {
      expected = Files.readAllBytes(Path.of(expectedPath + "/" + filename));
      output = Files.readAllBytes(Path.of(outputPath + "/" + filename));
      Assertions.assertEquals(expected.length, output.length, "Unexpected number of rows.");
      Assertions.assertTrue(Arrays.equals(output, expected), "Outputs are not equal.");
    } catch (IOException e) {
      e.printStackTrace();
      // System.out.println("hi");
    }
  }

  private void testPageEqualityHelper(String filename) {
    byte[] e;
    byte[] o;
    try {
      e = Files.readAllBytes(Path.of(expectedPath + "/" + filename));
      o = Files.readAllBytes(Path.of(outputPath + "/" + filename));
      Assertions.assertEquals(e.length, o.length, "Unexpected number of rows.");

      int expected[] = byteToIntArray(e);
      int output[] = byteToIntArray(o);

      for (int i = 0; i < expected.length; i += 1024) {
        int[] ex = Arrays.copyOfRange(expected, i, i + 1024);
        int[] op = Arrays.copyOfRange(output, i, i + 1024);

        Assertions.assertEquals(
            Arrays.toString(Arrays.copyOfRange(expected, i, i + 1024)),
            Arrays.toString(Arrays.copyOfRange(output, i, i + 1024)),
            "Outputs are not equal at page " + i / 1024);
      }
    } catch (IOException exception) {
      exception.printStackTrace();
      // System.out.println("hi");
    }
  }

  private int[] byteToIntArray(byte[] arr) {
    int[] result = new int[arr.length / 4];

    for (int i = 0; i < result.length; i++) {
      int n = 0;
      n += arr[3 + 4 * i];
      n += arr[2 + 4 * i] * 256;
      n += arr[1 + 4 * i] * 65536;
      n += arr[0 + 4 * i] * 16777216;
      result[i] = n;
    }

    return result;
  }

  // test sizes
  @Test
  public void testIndex1Size() throws ExecutionControl.NotImplementedException {
    testSizeHelper("Boats.E");
  }

  @Test
  public void testIndex2Size() throws ExecutionControl.NotImplementedException {
    testSizeHelper("Reserves.G");
  }

  @Test
  public void testIndex3Size() throws ExecutionControl.NotImplementedException {
    testSizeHelper("Sailors.A");
  }

  @Test
  public void testIndex4Size() throws ExecutionControl.NotImplementedException {
    testSizeHelper("Table1.K");
  }

  @Test
  public void testIndex5Size() throws ExecutionControl.NotImplementedException {
    testSizeHelper("Table2.N");
  }

  // test full files
  @Test
  public void testIndex1Full() throws ExecutionControl.NotImplementedException {
    testFullEqualityHelper("Boats.E");
  }

  @Test
  public void testIndex2Full() throws ExecutionControl.NotImplementedException {
    testFullEqualityHelper("Reserves.G");
  }

  @Test
  public void testIndex3Full() throws ExecutionControl.NotImplementedException {
    testFullEqualityHelper("Sailors.A");
  }

  @Test
  public void testIndex4Full() throws ExecutionControl.NotImplementedException {
    testFullEqualityHelper("Table1.K");
  }

  @Test
  public void testIndex5Full() throws ExecutionControl.NotImplementedException {
    testFullEqualityHelper("Table2.N");
  }

  // test by page
  @Test
  public void testIndex1ByPage() throws ExecutionControl.NotImplementedException {
    testPageEqualityHelper("Boats.E");
  }

  @Test
  public void testIndex2ByPage() throws ExecutionControl.NotImplementedException {
    testPageEqualityHelper("Reserves.G");
  }

  @Test
  public void testIndex3ByPage() throws ExecutionControl.NotImplementedException {
    testPageEqualityHelper("Sailors.A");
  }

  @Test
  public void testIndex4ByPage() throws ExecutionControl.NotImplementedException {
    testPageEqualityHelper("Table1.K");
  }

  @Test
  public void testIndex5ByPage() throws ExecutionControl.NotImplementedException {
    testPageEqualityHelper("Table2.N");
  }
}
