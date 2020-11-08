# Group By Hash
This folder contains implementations of "hash" GROUP BY. 

## Architecture
The main code used to generate the results files is [MainTest.java](final/src/main/java/MainTest.java).
We have done 3 implemtentations :
 - Single-Threaded Hash Group By, that can be found [there](final/src/main/java/raw_java/HashGroupBySingle.java).
 - Multi-Threaded Hash Group By, that can be found  [there](final/src/main/java/raw_java/PartitioningHashGroupBy.java).
 - Spark distributed Hash Group By, that can be found [there](final/src/main/java/spark/HashGroupBySpark.java).
