package common;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

public class OptimalSelection {

  private HashMap<String, ArrayList<Double>> costs;
  private HashMap<String, ArrayList<Integer>> columnInfo;
  private HashMap<String, Double> reductionInfo;
  public String tableName;
  private DBCatalog dbCatalog;

  public OptimalSelection() {
    this.costs = new HashMap<>();
    this.columnInfo = new HashMap<>();
    this.reductionInfo = new HashMap<>();
    this.dbCatalog = DBCatalog.getInstance();
  }

  public ArrayList<Expression> getBinaryExpressions(Expression e) {
    ArrayList<Expression> binaryExpressions = new ArrayList<>();
    if (e instanceof AndExpression) {
      AndExpression andExpression = (AndExpression) e;
      binaryExpressions.addAll(getBinaryExpressions(andExpression.getLeftExpression()));
      binaryExpressions.addAll(getBinaryExpressions(andExpression.getRightExpression()));
    } else if (e != null) {
      binaryExpressions.add(e);
    }
    return binaryExpressions;
  }

  public ArrayList<Object> getOptimalColumn(Expression expression, String tableName,
      HashMap<String, ArrayList<Integer>> indexInfo) {

    this.tableName = tableName;
    ArrayList<Expression> binaryExpressions = getBinaryExpressions(expression);

    for (Expression e : binaryExpressions) {
      if (e == null) {
        System.out.println("null");
      } else {
        System.out.println(e.toString());
      }
    }

    for (Expression e : binaryExpressions) {
      Expression left = ((BinaryExpression) e).getLeftExpression();
      Expression right = ((BinaryExpression) e).getRightExpression();

      System.out.println('"' + left.toString() + '"' + " " + '"' + right.toString() + '"');

      if (right instanceof Column && left instanceof Column) {
        System.out.println("both are columns");
        continue;
      } else if (left instanceof Column) {

        System.out.println("left is column");
        System.out.println("right is " + right.toString());

        System.out.println(right.getClass());

        int value = (int) ((LongValue) right).getValue();

        System.out.println("value: " + value);

        updateRange(left.toString(), value, e);
      } else if (right instanceof Column) {

        System.out.println("right is column");
        System.out.println("left is " + left.toString());
        int value = (int) ((LongValue) left).getValue();

        System.out.println("value: " + value);

        updateRange(right.toString(), value, e);
      }
    }

    System.out.println("columnInfo: " + columnInfo);

    System.out.println("ENTERING FOR LOOP");

    for (String column : columnInfo.keySet()) {

      System.out.println("column: " + column);

      String c1 = column.split("\\.")[1];

      boolean hasIndex = indexInfo.containsKey(c1);

      System.out.println(indexInfo);

      System.out.println("hasIndex: " + hasIndex);

      double indexCost = 0;

      double colMax = 0;
      double colMin = 0;

      ArrayList<String> tableStats = dbCatalog.getTableStats(tableName);

      for (int i = 0; i < tableStats.size(); i++) {
        if (tableStats.get(i).equals(c1)) {
          colMin = (double) (Integer.valueOf(tableStats.get(i + 1)));
          colMax = (double) (Integer.valueOf(tableStats.get(i + 2)));
          break;
        }
      }

      if (hasIndex) {

        ArrayList<Integer> tabInf = dbCatalog.getTabInfo(tableName);

        // pages
        int p = tabInf.get(0);
        // tuples
        int t = tabInf.get(1);
        // leaves

        String indexName = tableName + "." + c1;
        int l = dbCatalog.getNumLeavesOfIndex(indexName);

        ArrayList<Integer> allRange = columnInfo.get(column);
        int range = allRange.get(1) - allRange.get(0) + 1;

        // reduction factor
        double r = (double) range / (double) (colMax - colMin + 1);

        reductionInfo.put(column, r);

        // clustered
        boolean clustered = indexInfo.get(c1).get(0) == 1;

        System.out.println("p: " + p);
        System.out.println("t: " + t);
        System.out.println("l: " + l);
        System.out.println("r: " + r);
        System.out.println("range: " + range);
        System.out.println("clustered: " + clustered);

        // index cost
        if (clustered) {
          indexCost = 3 + p * r;
        } else {
          indexCost = 3 + l * r + p * r;
        }
      } else {
        indexCost = Integer.MAX_VALUE;
      }

      // size of the tuples
      int s = DBCatalog.getInstance().getAttributesPerTable(tableName);
      // tuples in base table
      int tb = (int) Double.parseDouble(tableStats.get(0));

      System.out.println("s: " + s);
      System.out.println("tb: " + tb);

      ArrayList<Integer> allRange2 = columnInfo.get(column);
      System.out.println(allRange2);
      double r2 = allRange2.get(1) - allRange2.get(0) + 1;

      System.out.println(r2);
      System.out.println(colMax + " " + colMin);

      if (!hasIndex)
        reductionInfo.put(column, ((1.0 * r2) / (1.0 * (colMax - colMin + 1))));

      // non index cost
      double nonIndexCost = 0;

      nonIndexCost = ((double) (tb * s)) / ((double) 4096);

      System.out.println("______________________");
      System.out.println("indexCost: " + indexCost);
      System.out.println("nonIndexCost: " + nonIndexCost);
      System.out.println("______________________");

      // if the non index cost is higher
      if (nonIndexCost > indexCost) {
        costs.put(column, new ArrayList<>(List.of(1.0, indexCost)));
      }
      // if the index cost is higher
      else {
        costs.put(column, new ArrayList<>(List.of(0.0, (nonIndexCost))));
      }
    }

    ArrayList<Object> lowestCost = new ArrayList<>();

    for (String column : costs.keySet()) {
      if (lowestCost.size() == 0) {
        // column
        lowestCost.add(column);
        // index or non index
        lowestCost.add(costs.get(column).get(0));
        // cost
        lowestCost.add(costs.get(column).get(1));
      } else {
        if (costs.get(column).get(1) < (double) lowestCost.get(2)) {
          lowestCost.set(0, column);
          lowestCost.set(1, costs.get(column).get(0));
          lowestCost.set(2, costs.get(column).get(1));
        }
      }
    }

    lowestCost.add(reductionInfo);

    System.out.println(lowestCost);
    System.out.println("RETURNING");
    return lowestCost;
  }

  public void updateRange(String column, Integer value, Expression expression) {

    // ArrayList<String> tableStats =
    // DBCatalog.getInstance().getTableStats(tableName);
    ArrayList<String> tableStats = dbCatalog.getTableStats(tableName);
    ArrayList<Integer> range = new ArrayList<>();
    System.out.println("updateRange(column, value, expression");
    System.out.println("ts: " + tableStats);
    System.out.println(columnInfo);
    System.out.println(column);

    if (columnInfo.containsKey(column)) {
      range = columnInfo.get(column);
    } else {
      for (int i = 0; i < tableStats.size(); i++) {
        String col = column.split("\\.")[1];

        if (tableStats.get(i).equals(col)) {
          range.add(Integer.valueOf(tableStats.get(i + 1)));
          range.add(Integer.valueOf(tableStats.get(i + 2)));
          break;
        }
      }
    }

    System.out.println("range: " + range);

    if (expression instanceof GreaterThan) {
      if (value > range.get(0)) {
        range.set(0, value);
      }
    } else if (expression instanceof GreaterThanEquals) {
      if (value >= range.get(0)) {
        range.set(0, value);
      }
    } else if (expression instanceof MinorThan) {
      if (value < range.get(1)) {
        range.set(1, value);
      }
    } else if (expression instanceof MinorThanEquals) {
      if (value <= range.get(1)) {
        range.set(1, value);
      }
    } else if (expression instanceof EqualsTo) {
      range.set(0, value);
      range.set(1, value);
    } else if (expression instanceof NotEqualsTo) {
      if (value != range.get(0)) {
        range.set(0, value);
      }
    }

    System.out.println(range);
    System.out.println(expression.getClass());

    columnInfo.put(column, range);
  }

}