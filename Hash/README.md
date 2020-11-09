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
  
The architecture inside the modules are quite similar but are differ in several points, especially the Spark implementation required some adaptations in order to work with the Spark framework.

## Implementation details
### Single Threaded
It is important to understand the concepts involved in the single-threaded version, because both multi-processors and spark implementation borrow from it. The algorithm works in two passes. One is to fill a hashtable with the aggregated groups values. The other one simply outputs the groups. From this, one can see that the most interesting part is the hashtable, and how to insert, retrieve and update values. See the report to get more details about the hashtable implementation.

### Multithreaded Implementation
*We implemented three versions of the multiprocessor algorithm. We only kept the best performing option for the measures. *

Our implementation of the partitioning hash table starts by creating a new hash-table for each thread. There is no need for a merge function. Instead, each thread will only input in its own hashtable the records that belong to its partition. The partition is determined by a modular hashing on the group, by the number of threads. This way, no two records belonging to the same groups can be managed by different threads. 
Because of this careful partitioning, there is no need to merge outputs. Each thread can start outputting groups as soon as it is finished.

### Spark Implementation
The Spark implementation uses only function of the main Spark module and not the SparkSQL module.
In fact, apart from the functions used to parse the input files we only used two functions :
- **mapPartitions** which splits the input for the different workers and perform an operation on each partition. 
- **reduce** that take the results of **mapPartitions** and group 2 by 2 the output until we obtain a final result.

All of the others operations are handmade and are based on what was implemented for the singlethreaded and multithreaded algorithms, though it has some small differences that are not supposed to influence performance (e.g. we hash on String rather than Integer).

## Results

Test were runned on two folders of files, with variables **group** distribution and **size**.
The folders can be found there:
 - [Size test](/dataGen/size)
 - [Group test](/dataGen/group)
 
 The results can be found there:
 - [Size results](final/result_size.csv)
 - [Group results](final/result_group.csv)
 
 Results format are : **file**;**timeSingleThreaded**;**timeMultiThreaded**;**timeSpark**
 
 ### Plots
Below you can find the results of the tests for the two folders (**size** and **group**).
 ![](img/size.png)
 ![](img/group.png)

### Analysis
 - For the **size test**, what we can see is that the Spark implementation outclasses the two others when the amount of data is small a
than is way worse when the data is huge. Considering these test have been run on a computer with 4 cores, using 4 threads in parallel we can assume that it would have been faster on more cores, as Spark is a language made to scale. Between the single-threaded and the multi-threaded implementation, the single threaded seem to be more consistent as the data size increases. It can be explained by the fact that every partition has to store a huge CustomHashTable which can slow the process while the computer has to deal with multiple threads.

 - For the **group test**, the Spark implementation is almost always worse than the others. Considering the implementations are very similar, it is, we suppose, due to the Spark process in itself, and the fact that our class CustomHashTable is not optimized to work with it. The two others implementations seem to be quite independant from the group size, and quite similar in there performances.

