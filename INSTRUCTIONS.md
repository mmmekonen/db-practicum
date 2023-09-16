## Team Members
Jeffery Lipson (jal496), Maelat Mekonen (mmm432), Richard Fischer (rtf48)

## Top-Level Class
Compiler.java

## Extracting Join Conditions
First, we use the ExpressionSorter and ExpressionVisitor
classes to extract the sub-conditions that only reference 
one table, and concatenate the conditions that reference
the same tables together. Then, we create an operator
for each table mentioned in the statement, that selects
tuples based off of the conditions we extracted. Finally,
we recursively call the join operator on the individual-
table operators to join them together, and apply our final
operations (project, sort, distinct) to its output.