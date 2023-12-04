package compiler;

import common.DBCatalog;
import common.QueryPlanBuilder;
import common.TreeIndex;
import common.Tuple;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.apache.logging.log4j.*;
import physical_operator.Operator;
import physical_operator.ScanOperator;

/**
 * Top level harness class; reads queries from an input file one at a time, processes them and sends
 * output to file or to System depending on flag.
 */
public class Compiler {
  private static final Logger logger = LogManager.getLogger();

  private static String configFile;
  private static String outputDir;
  private static String inputDir;
  private static String tempDir;
  private static final boolean outputToFiles = true; // true = output to

  // files, false = output
  // to System.out

  /**
   * Reads statements from queriesFile one at a time, builds query plan and evaluates, dumping
   * results to files or console as desired.
   *
   * <p>If dumping to files result of ith query is in file named queryi, indexed stating at 1.
   */
  public static void main(String[] args) {

    // Set up configs for which type of sort and join to use
    configFile = args[0];
    readConfigFile();
    DBCatalog db = DBCatalog.getInstance();
    db.setDataDirectory(inputDir + "/db");
    db.setSortDirectory(tempDir);
    db.setIndexDirectory(inputDir + "/db/indexes");
    db.setIndexInfo();

    try {
      if (outputToFiles) {
        for (File file : (new File(outputDir).listFiles())) file.delete(); // clean output directory
      }

      // gather statistics
      gatherStats();
      db.setStats(true);
      logger.info("Created database statistics");

      for (File file : (new File(inputDir + "/db/indexes").listFiles()))
        file.delete(); // clean index directory

      // Set up indexes
      logger.info("Building indexes...");
      ArrayList<String> tables = new ArrayList<>();
      tables.addAll(db.getIndexInfo().keySet());
      for (int i = 0; i < tables.size(); i++) {
        HashMap<String, ArrayList<Integer>> colmap = db.getIndexInfo().get(tables.get(i));
        for (String col : colmap.keySet()) {
          ArrayList<Integer> info = colmap.get(col);

          TreeIndex t =
              new TreeIndex(
                  inputDir, tables.get(i), col, db.findColumnIndex(tables.get(i), col), info);
        }
      }

      logger.info("Indexes have been built");

      // run queries
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
          Operator plan = queryPlanBuilder.buildPlan(statement);

          if (outputToFiles) {
            // output data
            File outfile = new File(outputDir, "query" + counter);
            long timeElapsed = System.currentTimeMillis();
            plan.dump(outfile);
            timeElapsed = System.currentTimeMillis() - timeElapsed;
            logger.info("Query processing time: " + timeElapsed + "ms");

            // output logical plan
            File logicalPlanFile = new File(outputDir, "query" + counter + "_logicalplan");
            FileWriter writer = new FileWriter(logicalPlanFile);
            writer.write(queryPlanBuilder.logicalString(statement));
            writer.close();

            // output physical plan
            File physicalPlanFile = new File(outputDir, "query" + counter + "_physicalplan");
            writer = new FileWriter(physicalPlanFile);
            writer.write(plan.toString());
            writer.close();

          } // else {
          // plan.dump(System.out);
          // }
        } catch (Exception e) {
          logger.error(e.getMessage());
        }

        ++counter;
      }
    } catch (Exception e) {
      System.err.println("Exception occurred in interpreter");
      logger.error(e.getMessage());
    }
  }

  /** Reads the config file passed through the command line and sets all the directories. */
  private static void readConfigFile() {
    try {
      Scanner s = new Scanner(new File(configFile));
      inputDir = s.nextLine();
      outputDir = s.nextLine();
      tempDir = s.nextLine();
    } catch (Exception e) {
      System.out.println(e + ": Could not find config file");
    }
  }

  /** Computes the statistics for each relation in the database. */
  private static void gatherStats() {
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(inputDir + "/db/stats.txt")); ) {
      DBCatalog db = DBCatalog.getInstance();
      Set<String> tableNames = db.getTableNames();

      // store data
      HashMap<Integer, Integer> min = new HashMap<>();
      HashMap<Integer, Integer> max = new HashMap<>();
      int len = 0;
      int pid = 0;

      // whether or not to write a newline in stats.txt
      boolean newline = false;
      for (String table : tableNames) {
        ArrayList<Column> columns = db.getTableSchema(table);
        Operator op = new ScanOperator(table, null);
        Tuple tuple;
        while ((tuple = op.getNextTuple()) != null) {
          for (int i = 0; i < columns.size(); i++) {
            min.put(
                i,
                min.get(i) == null
                    ? tuple.getElementAtIndex(i)
                    : Math.min(min.get(i), tuple.getElementAtIndex(i)));
            max.put(
                i,
                max.get(i) == null
                    ? tuple.getElementAtIndex(i)
                    : Math.max(max.get(i), tuple.getElementAtIndex(i)));
          }
          len++;
          pid = tuple.getPID();
        }

        pid++;
        DBCatalog.getInstance().setNumPages(table, pid, len);

        // write to file
        if (newline) {
          bw.newLine();
        }
        bw.write(table + " " + len);
        for (int i = 0; i < columns.size(); i++) {
          bw.write(" " + columns.get(i).getColumnName() + "," + min.get(i) + "," + max.get(i));
        }
        newline = true;

        // clear hashmaps and length
        min.clear();
        max.clear();
        len = 0;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
