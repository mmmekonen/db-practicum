## Team Members
Jeffery Lipson (jal496), Maelat Mekonen (mmm432), Richard Fischer (rtf48)

## Top-Level Class
Compiler.java

## Index Scan Operator
For the index scan operator, the lowkey and highkey are set in the constructor. If highkey is null, it is set to the max int value. If lowkey is null, we start from the first data entry (which will be on page 1). If lowkey is not null, we deserialize the tree, starting at the address of the root node. From this, depending on wether or not the node is an index or a leaf node, we search the indexes/leaves until we find a key that is greater than or equal to the lowkey (then we are at the data entry we need to start at). This will only search indexes and leaves that we would possibly use, and does not deserialize the whole tree. In the construction of the index scan operator, we scan the index file to find out if the index is clustered or unclustered. Then, in the calls to getNextTuple(), we check if the index is clustered or not. If clustered, we simply call getNextTuple() on a scan operator of the sorted file. If unclustered, we get the next rid and get the tuple at that corresponding pageID, tupleID. 


## Other Information - SMJ
Unfortunately, we were unable to fully implement the SMJ operator. However, the external sort maintains a 
bounded state by ensuring that only as many tuples as can fit in the buffer (specified by the user) are stored in 
main memory at any given time.