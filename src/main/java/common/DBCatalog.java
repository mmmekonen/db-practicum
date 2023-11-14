package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to contain information about database - names of tables, schema of each
 * table and file
 * where each table is located. Uses singleton pattern.
 *
 * <p>
 * Assumes dbDirectory has a schema.txt file and a /data subdirectory containing
 * one file per
 * relation, named "relname".
 *
 * <p>
 * Call by using DBCatalog.getInstance();
 */
public class DBCatalog {
  private final Logger logger = LogManager.getLogger();

  private final HashMap<String, ArrayList<Column>> tables;
  private HashMap<String, HashMap<String, ArrayList<Integer>>> indexInfo;
  private boolean useIndexes;
  private static DBCatalog db;

  private String dbDirectory;
  private String sortDirectory;
  private String indexDirectory;

  /** Reads schemaFile and populates schema information */
  private DBCatalog() {
    tables = new HashMap<>();
    indexInfo = new HashMap<>();
  }

  /**
   * Instance getter for singleton pattern, lazy initialization on first
   * invocation
   *
   * @return unique DB catalog instance
   */
  public static DBCatalog getInstance() {
    if (db == null) {
      db = new DBCatalog();
    }
    return db;
  }

  /**
   * Sets the data directory for the database catalog.
   *
   * @param directory: The input directory.
   */
  public void setDataDirectory(String directory) {
    try {
      dbDirectory = directory;
      BufferedReader br = new BufferedReader(new FileReader(directory + "/schema.txt"));
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\\s");
        String tableName = tokens[0];
        ArrayList<Column> cols = new ArrayList<Column>();
        for (int i = 1; i < tokens.length; i++) {
          cols.add(new Column(new Table(null, tableName), tokens[i]));
        }
        tables.put(tokens[0], cols);
      }
      br.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  /** Sets the info for all the indexes as defined in index_info.txt. */
  public void setIndexInfo() {
    try {
      Scanner s = new Scanner(new File(dbDirectory + "/index_info.txt"));
      while (s.hasNextLine()) {
        String[] params = s.nextLine().split(" ");
        if (indexInfo.containsKey(params[0])) {
          HashMap<String, ArrayList<Integer>> colmap = indexInfo.get(params[0]);
          colmap.put(params[1], new ArrayList<>(List.of(Integer.valueOf(params[2]), Integer.valueOf(params[3]))));
        } else {
          HashMap<String, ArrayList<Integer>> colmap = new HashMap<>();
          colmap.put(params[1], new ArrayList<>(List.of(Integer.valueOf(params[2]), Integer.valueOf(params[3]))));
          indexInfo.put(params[0], colmap);
        }
      }
    } catch (Exception e) {
      System.out.println(e + ": Could not find index_info file");
    }
  }

  /**
   * Returns the hashmap of ArrayLists of Strings of the column, cluster flag, and
   * tree order for
   * the index on each of the tables.
   *
   * @return a hashmap containing the index info.
   */
  public HashMap<String, HashMap<String, ArrayList<Integer>>> getIndexInfo() {
    return indexInfo;
  }

  /**
   * Sets the boolean value as to whether indexes are to be used for selection or
   * not.
   *
   * @param val true if indexes are to be used.
   */
  public void setUseIndex(boolean val) {
    useIndexes = val;
  }

  /**
   * Returns a boolean value as to whether indexes are to be used for selection or
   * not.
   *
   * @return true if indexes are to be used.
   */
  public boolean useIndexes() {
    return useIndexes;
  }

  /**
   * Sets the temporary directory used by external sort.
   *
   * @param directory: The temporary directory.
   */
  public void setSortDirectory(String directory) {
    sortDirectory = directory;
  }

  /**
   * Gets the path of the temporary directory used by external sort.
   *
   * @return string of the directory's path.
   */
  public String getSortDirectory() {
    return sortDirectory;
  }

  /**
   * Sets the path of the index directory used to store indexes.
   *
   * @param directory string of the indexes file path.
   */
  public void setIndexDirectory(String directory) {
    indexDirectory = directory;
  }

  /**
   * Gets the path of the index directory used to store indexes.
   *
   * @return string of the indexes file path.
   */
  public String getIndexDirectory() {
    return indexDirectory;
  }

  /**
   * Gets path to file where a particular table is stored
   *
   * @param tableName table name
   * @return file where table is found on disk
   */
  public File getFileForTable(String tableName) {
    return new File(dbDirectory + "/data/" + tableName);
  }

  /**
   * Returns of copy of the schema for the specified table
   *
   * @param tableName table name
   * @return a list of JSQL columns
   */
  public ArrayList<Column> getTableSchema(String tableName) {
    ArrayList<Column> schema = new ArrayList<>();
    for (Column c : tables.get(tableName)) {
      schema.add(new Column(new Table(null, tableName), c.getColumnName()));
    }
    return schema;
    // return tables.get(tableName);
  }

  /**
   * Finds the index of a column of a given table, as it is found in the table
   * schema
   *
   * @param tableName  The table holding the column
   * @param columnName The column whose index is to be located
   * @return The index of the column
   */
  public int findColumnIndex(String tableName, String columnName) {
    ArrayList<String> schema = new ArrayList<>();
    for (Column c : tables.get(tableName)) {
      schema.add(c.getColumnName());
    }
    return schema.indexOf(columnName);
  }
}
