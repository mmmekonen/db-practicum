## Team Members
Jeffery Lipson (jal496), Maelat Mekonen (mmm432), Richard Fischer (rtf48)

## Top-Level Class
Compiler.java

## SMJ & Distinct
The DISTINCT operator only keeps a single tuple at a time in main memory, so it doesn't keep an unbounded state.

## SMJ
Unfortunately, we were unable to fully implement the SMJ operator. However, the external sort maintains a 
bounded state by ensuring that only as many tuples as can fit in the buffer (specified by the user) are stored in 
main memory at any given time.