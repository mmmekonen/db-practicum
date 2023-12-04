## Team Members
Jeffery Lipson (jal496), Maelat Mekonen (mmm432), Richard Fischer (rtf48)

## Top-Level Class
Compiler.java

## Selection Pushing
an explanation of where the implementation is found (i.e. which classes/methods perform it), as well as an explanation of your logic

## Implementation For Each Logical Selection Operator
an explanation of where the implementation is found (i.e. which classes/methods perform it), as well as an explanation of your logic

## Index/Non-index Choice
The code determines whether or not to choose an index based on the cost function described in 3.3. (Tuples in base * size of tuple / 4096 for full scan, 3 + p * r for clustered index and 3 + l * r + p * r). Then, the best path is chosen based on which provides the lowest cost.

## Join Order
The code that determines the optimal Join order can be found in **DetermineJoinOrder.java**. There, above the class definition, is a comment explaining all of the logic for determining the join order.

## Implementation For Each Join Operator
Our Join Operator implementation can be found in **PhysicalPlanBuilder.java** under the Join visitor method. We decided to use BNLJ for all of our Join Operators as we saw in P2's benchmarking that it consistently ran faster than TNLJ on all queries tested. SMJ is not used as a Join Operator in our implementation as our SMJ implementation currently does not produce correct query results.

## Note
Given that the join order varies based off the data statistics, the values in the output tuple may not be in the order tables listed in the query, but instead be based off of the join order. This means, for a query like `SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;`, the output tuples may have the form of `(<Reserves-tuples>,<Sailors-tuples)` instead of the expected `(<Sailors-tuples>,<Reserves-tuples>)` (for a non-optimized join order), if Reserves is the smaller relation.