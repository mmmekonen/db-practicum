package compiler;

import common.DBCatalog;
import common.QueryPlanBuilder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.apache.logging.log4j.*;
import physical_operator.Operator;

/**
 * Top level harness class; reads queries from an input file one at a time,
 * processes them and sends
 * output to file or to System depending on flag.
 */
public class Compiler {
  private static final Logger logger = LogManager.getLogger();

  private static String configFile;
  private static String outputDir;
  private static String inputDir;
  private static String tempDir;
  private static final boolean outputToFiles = true; // true = output to

  private static int buildIndexes;
  private static int evalQueries;

  private static int joinType;
  private static int joinBuffer;
  private static int sortType;
  private static int sortBuffer;

  // files, false = output
  // to System.out

  /**
   * Reads statements from queriesFile one at a time, builds query plan and
   * evaluates, dumping
   * results to files or console as desired.
   *
   * <p>
   * If dumping to files result of ith query is in file named queryi, indexed
   * stating at 1.
   */
  public static void main(String[] args) {

    configFile = args[0];
    readConfigFile();
    setConfig();
    DBCatalog db = DBCatalog.getInstance();
    db.setDataDirectory(inputDir + "/db");
    db.setSortDirectory(tempDir);
    db.setIndexInfo();

    try {
      if (outputToFiles) {
        for (File file : (new File(outputDir).listFiles()))
          file.delete(); // clean output directory
      }

      if (buildIndexes == 1) {
        logger.info("Building indexes...");
        // TODO: build indexes if needed
        logger.info("Indexes have been built");
      }

      // check if building queries
      if (evalQueries == 1) {
        String str = Files.readString(Path.of(inputDir + "/queries.sql"));
        Statements statements = CCJSqlParserUtil.parseStatements(str);
        QueryPlanBuilder queryPlanBuilder = new QueryPlanBuilder();

        int counter = 1; // for numbering output files
        for (Statement statement : statements.getStatements()) {
          // clean temp directory before each query
          for (File file : (new File(tempDir).listFiles())) {
            if (file.isDirectory()) {
              for (File f : file.listFiles()) {
                f.delete();
              }
            }
            file.delete();
          }

          logger.info("Processing query: " + statement);

          try {
            Operator plan = queryPlanBuilder.buildPlan(statement, joinType, joinBuffer, sortType, sortBuffer);

            if (outputToFiles) {
              File outfile = new File(outputDir + "/query" + counter);
              long timeElapsed = System.currentTimeMillis();
              plan.dump(outfile);
              timeElapsed = System.currentTimeMillis() - timeElapsed;
              logger.info("Query processing time: " + timeElapsed + "ms");
            } // else {
            // plan.dump(System.out);
            // }
          } catch (Exception e) {
            logger.error(e.getMessage());
          }

          ++counter;
        }
      }
    } catch (Exception e) {
      System.err.println("Exception occurred in interpreter");
      logger.error(e.getMessage());
    }
  }

  /**
   * Reads the config file passed through the command line and sets all the
   * parameters for how queries should be built.
   */
  private static void readConfigFile() {
    try {
      Scanner s = new Scanner(new File(configFile));
      inputDir = s.nextLine();
      outputDir = s.nextLine();
      tempDir = s.nextLine();
      buildIndexes = s.nextInt();
      evalQueries = s.nextInt();
    } catch (Exception e) {
      System.out.println(e + ": Could not find config file");
    }
  }

  /** helper function to read in settings from the plan_builder_config.txt file */
  private static void setConfig() {
    try {
      Scanner s = new Scanner(new File(inputDir + "/plan_builder_config.txt"));
      joinType = s.nextInt();
      if (joinType == 1)
        joinBuffer = s.nextInt();
      sortType = s.nextInt();
      if (sortType == 1)
        sortBuffer = s.nextInt();
    } catch (Exception e) {
      System.out.println(e + ": Could not find config file");
    }
  }
}
