# Group By Hash
This folder contains implementations of "hash" GROUP BY. 

## Architecture
The main code used to generate the results files is [MainTest.java](final/src/main/java/MainTest.java).
We have done 3 implemtentations :
 - Single-Threaded Hash Group By, that can be found [there](final/src/main/java/raw_java/HashGroupBySingle.java).
 - Multi-Threaded Hash Group By, that can be found  [there](final/src/main/java/raw_java/PartitioningHashGroupBy.java).
 - Spark distributed Hash Group By, that can be found [there](final/src/main/java/spark/HashGroupBySpark.java).
 The Spark module and the raw Java module are separated :
  - [Raw Java module](final/src/main/java/raw_java)
  - [Spark module](final/src/main/java/spark)
The architecture inside the modules are quite similar but are different in several points, especially the Spark implementation required some modifications in order to work properly.

### Single Threaded
### Spark Implementation
The Spark implementation uses only function of the main Spark module and not the SparkSQL module.
In fact, apart from the functions used to parse the input files we only used two functions :
- **mapPartitions** which splits the input for the different workers and perform an operation on each partition. 
- **reduce** that take the results of **mapPartitions** and group 2 by 2 the output until we obtain a final result.

