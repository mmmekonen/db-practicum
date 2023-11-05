import common.DBCatalog;
import common.TreeIndex;

import java.io.File;
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

public class P3IndexTests {
  private static String expectedPath;
  private static String outputPath;
  private static String tempDir = "src/test/resources/samples/temp";

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {

    ClassLoader classLoader = P3IndexTests.class.getClassLoader();
    String path = Objects.requireNonNull(classLoader.getResource("samples/input")).getPath();

    DBCatalog db = DBCatalog.getInstance();
    db.setDataDirectory(path + "/db");
    db.setSortDirectory(tempDir);
    db.setIndexInfo();

    expectedPath = "src/test/resources/samples/expected_indexes";
    outputPath = "src/test/resources/samples/input/db/indexes";

    // make indexes
    ArrayList<String> tables = new ArrayList<>();
    tables.addAll(db.getIndexInfo().keySet());

    for (int i = 0; i < tables.size(); i++) {

      ArrayList<String> info = db.getIndexInfo().get(tables.get(i));

      ScanOperator base = new ScanOperator(tables.get(i), null);
      ArrayList<Column> temp = new ArrayList<>();
      temp.add(new Column(new Table(null, tables.get(i)), info.get(0)));
      InMemorySortOperator op = new InMemorySortOperator(base, temp);

      TreeIndex t = new TreeIndex("src/test/resources/samples/input/db/indexes/" + tables.get(i) + "."
              + info.get(0), op, Integer.parseInt(info.get(2)), db.findColumnIndex(tables.get(i),
              info.get(0)), Integer.valueOf(info.get(1)) == 1 ? true : false);
    }
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
      //System.out.println("hi");
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
      //System.out.println("hi");
    }
  }

  private void testPageEqualityHelper(String filename) {
    byte[] e;
    byte[] o;
    try {
      e = Files.readAllBytes(Path.of(expectedPath + "/" + filename));
      o = Files.readAllBytes(Path.of(outputPath + "/" + filename));
      Assertions.assertEquals(e.length, o.length, "Unexpected number of rows.");


      int expected[] = new int[e.length / 4];
      for(int i = 0; i < expected.length; i++) {
        expected[i] = (e[3 + 4 * i] & 0xFF) | ((e[2 + 4 * i] & 0xFF) << 8) |
                ((e[1 + 4 * i] & 0xFF) << 16) | ((e[0 + 4 * i] & 0xFF) << 24);
      }

      int output[] = new int[o.length / 4];
      for(int i = 0; i < output.length; i++) {
        expected[i] = (o[3 + 4 * i] & 0xFF) | ((o[2 + 4 * i] & 0xFF) << 8) |
                ((o[1 + 4 * i] & 0xFF) << 16) | ((o[0 + 4 * i] & 0xFF) << 24);
      }

      for (int i = 0; i < expected.length; i += 1024) {
        Assertions.assertEquals(Arrays.copyOfRange(output, i, i + 1024),
                Arrays.copyOfRange(expected, i, i + 1024), "Outputs are not equal at page " + i / 1024);
      }
    } catch (IOException exception) {
      exception.printStackTrace();
      //System.out.println("hi");
    }
  }

  @Test
  public void testIndex1Size() throws ExecutionControl.NotImplementedException {
    testSizeHelper("Boats.E");
  }

  @Test
  public void testIndex2Size() throws ExecutionControl.NotImplementedException {
    testSizeHelper("Sailors.A");
  }

  @Test
  public void testIndex1Full() throws ExecutionControl.NotImplementedException {
    testFullEqualityHelper("Boats.E");
  }

  @Test
  public void testIndex2Full() throws ExecutionControl.NotImplementedException {
    testFullEqualityHelper("Sailors.A");
  }

  @Test
  public void testIndex1ByPage() throws ExecutionControl.NotImplementedException {
    testPageEqualityHelper("Boats.E");
  }

  @Test
  public void testIndex2ByPage() throws ExecutionControl.NotImplementedException {
    testPageEqualityHelper("Sailors.A");
  }

}
