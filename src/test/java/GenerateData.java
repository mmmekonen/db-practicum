import common.Tuple;
import common.TupleWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GenerateData {
  private static String inputDir = "src/test/resources/samples/input2/db/data";

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException {

    ClassLoader classLoader = GenerateData.class.getClassLoader();
    Random numGenerator = new Random();
    for (int i = 1; i < 4; i++) {
      File file = new File(inputDir, "test" + i);
      TupleWriter tw = new TupleWriter(file);
      for (int tupleCount = 0; tupleCount < 5000; tupleCount++) {
        tw.writeTuple(
            new Tuple(
                new ArrayList<Integer>(
                    List.of(
                        numGenerator.nextInt(201),
                        numGenerator.nextInt(201),
                        numGenerator.nextInt(201)))));
      }
      tw.close();
    }
  }

  @Test
  public void done() {
    System.out.println("Done.");
  }
}
