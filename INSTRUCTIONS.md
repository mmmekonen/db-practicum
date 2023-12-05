## Team Members
Jeffery Lipson (jal496), Maelat Mekonen (mmm432), Richard Fischer (rtf48)

## Top-Level Class
Compiler.java

## Selection Pushing
Selection pushing is found in the **QueryPlanBuilder.java** file, using the **SelectUF.java** and **UFElement.java** files. Instead of breaking the query down into a handful of select and join queries like it did previously, the QueryPlanBuilder creates a union-find data structure of the bounds on each attribute set by the query, and uses those bounds to directly inform the selection operators instead of applying them piecemeal through a series of joins.

## Implementation For Each Logical Selection Operator
Logical operators are contained in the **logical_operator** folder. The only information they contain is their child (or children) operators, and the data necessary to perform their operation, such as a WHERE expression for selection and join operators, and a list of columns for sort or projection operators.

## Index/Non-index Choice
The code determines whether or not to choose an index based on the cost function described in 3.3 and can be found in **OptimalSelection.java** and **QueryPlanBuilder.java** under the select visitor method. (Tuples in base * size of tuple / 4096 for full scan, 3 + p * r for clustered index and 3 + l * r + p * r). Then, the best path is chosen based on which provides the lowest cost.

## Join Order
The code that determines the optimal Join order can be found in **DetermineJoinOrder.java**. There, above the class definition, is a comment explaining all of the logic for determining the join order.

## Implementation For Each Join Operator
Our Join Operator implementation can be found in **PhysicalPlanBuilder.java** under the Join visitor method. We decided to use BNLJ for all of our Join Operators as we saw in P2's benchmarking that it consistently ran faster than TNLJ on all queries tested. SMJ is not used as a Join Operator in our implementation as our SMJ implementation currently does not produce correct query results.

## Note
Given that the join order varies based off the data statistics, the values in the output tuple may not be in the order tables listed in the query, but instead be based off of the join order. This means, for a query like `SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;`, the output tuples may have the form of `(<Reserves-tuples>,<Sailors-tuples)` instead of the expected `(<Sailors-tuples>,<Reserves-tuples>)` (for a non-optimized join order), if Reserves is the smaller relation.