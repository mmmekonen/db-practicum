## Team Members
Jeffery Lipson (jal496), Maelat Mekonen (mmm432), Richard Fischer (rtf48)

## Top-Level Class
Compiler.java

## Selection Pushing
an explanation of where the implementation is found (i.e. which classes/methods perform it), as well as an explanation of your logic

## Implementation For Each Logical Selection Operator
an explanation of where the implementation is found (i.e. which classes/methods perform it), as well as an explanation of your logic

## Join Order
The code that determines the optimal Join order can be found in DetermineJoinOrder.java. There, above the class definition, is a comment explaining all of the logic for determining the join order.

## Implementation For Each Join Operator
Our Join Operator implementation can be found in PhysicalPlanBuilder.java under the Join visitor method. We decided to use BNLJ for all of our Join Operators as we saw in P2's benchmarking that it consistently ran faster than TNLJ on all queries tested. SMJ is not used as a Join Operator in our implementation as our SMJ implementation currently does not produce correct query results.