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

  private HashMap<String, ArrayList<Integer>> costs;
  private HashMap<String, ArrayList<Integer>> columnInfo;
  public String tableName;

  public OptimalSelection() {
    this.costs = new HashMap<>();
    this.columnInfo = new HashMap<>();
  }

  public ArrayList<Expression> getBinaryExpressions(Expression e) {
    ArrayList<Expression> binaryExpressions = new ArrayList<>();
    if (e instanceof AndExpression) {
      AndExpression andExpression = (AndExpression) e;
      binaryExpressions.addAll(getBinaryExpressions(andExpression.getLeftExpression()));
      binaryExpressions.addAll(getBinaryExpressions(andExpression.getRightExpression()));
    } else {
      binaryExpressions.add(e);
    }
    return binaryExpressions;
  }

  public ArrayList<Object> getOptimalColumn(Expression expression, String tableName,
      HashMap<String, ArrayList<Integer>> indexInfo) {

    this.tableName = tableName;
    ArrayList<Expression> binaryExpressions = getBinaryExpressions(expression);
    for (Expression e : binaryExpressions) {
      Expression left = ((BinaryExpression) e).getLeftExpression();
      Expression right = ((BinaryExpression) e).getRightExpression();

      System.out.println('"' + left.toString() + '"' + " " + '"' + right.toString() + '"');

      if (left instanceof Column) {

        System.out.println("left is column");
        System.out.println("right is " + right.toString());

        System.out.println(right.getClass());

        int value = (int) ((LongValue) right).getValue();

        System.out.println("value: " + value);

        updateRange(left.toString(), value, expression);
      } else if (right instanceof Column) {

        System.out.println("right is column");
        System.out.println("left is " + left.toString());
        int value = (int) ((LongValue) left).getValue();

        System.out.println("value: " + value);

        updateRange(right.toString(), value, expression);
      }
    }

    System.out.println("columnInfo: " + columnInfo);

    for (String column : columnInfo.keySet()) {

      // pages
      int p = 0;
      // tuples
      int t = 0;
      // leaves
      int l = 0;

      ArrayList<Integer> allRange = columnInfo.get(column);
      int range = allRange.get(1) - allRange.get(0) + 1;

      // reduction factor
      int r = range / t;

      // clustered
      boolean clustered = false;

      double indexCost = 0;

      // index cost
      if (clustered) {
        indexCost = 3 + p * r;
      } else {
        indexCost = 3 + l * r + p * r;
      }

      // size of tuple
      int s = 0;
      // tuples in base table
      int tb = 0;

      // non index cost
      double nonIndexCost = 0;

      nonIndexCost = (tb * s) / 4096;

      // if the non index cost is higher
      if (nonIndexCost > indexCost) {
        costs.put(column, new ArrayList<>(List.of(1, (int) indexCost)));
      }
      // if the index cost is higher
      else {
        costs.put(column, new ArrayList<>(List.of(0, (int) nonIndexCost)));
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
        if (costs.get(column).get(1) < (int) lowestCost.get(2)) {
          lowestCost.set(0, column);
          lowestCost.set(1, costs.get(column).get(0));
          lowestCost.set(2, costs.get(column).get(1));
        }
      }
    }

    return lowestCost;
  }

  public void updateRange(String column, Integer value, Expression expression) {
    ArrayList<String> tableStats = DBCatalog.getInstance().getTableStats(tableName);
    ArrayList<Integer> range = new ArrayList<>();

    System.out.println("ts: " + tableStats);

    if (columnInfo.containsKey(column)) {
      range = columnInfo.get(column);
    } else {
      for (int i = 0; i < tableStats.size(); i++) {
        String col = column.split(".")[1];
        if (tableStats.get(i).equals(col)) {
          range.add(Integer.valueOf(tableStats.get(i + 1)));
          range.add(Integer.valueOf(tableStats.get(i + 2)));
          break;
        }
      }
    }

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

    columnInfo.put(column, range);
  }

}