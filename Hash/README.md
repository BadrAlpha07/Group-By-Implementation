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
### Spark Implementation
The Spark implementation uses only function of the main Spark module and not the SparkSQL module.
In fact, apart from the functions used to parse the input files we only used two functions :
- **mapPartitions** which splits the input for the different workers and perform an operation on each partition. 
- **reduce** that take the results of **mapPartitions** and group 2 by 2 the output until we obtain a final result.

All of the others operations are handmade, especially the implementation of the custom hashtable and the operations on the hashtables (merge records, merge tables, collision management, restructuration if table is full).
Like the two others implementations, this algorithm relies heavily of our custom implementation of the HashTable and so are the performances.

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
 ![](img/group.png)
 ![](img/size.png)

### Analysis
 - For the **size test**, what we can see is that the Spark implementation outclasses the two others when the amount of data is small a
than is way worse when the data is huge. Considering these test have been run on a computer with 4 cores, using 4 threads in parallel we can assume that it would have been faster on more cores, as Spark is a language made to scale. Between the single-threaded and the multi-threaded implementation, the single threaded seem to be more consistent as the data size increases. It can be explained by the fact that every partition has to store a huge CustomHashTable which can slow the process while the computer has to deal with multiple threads.

 - For the **group test**, the Spark implementation is almost always worse than the others. Considering the implementations are very similar, it is, we suppose, due to the Spark process in itself, and the fact that our class CustomHashTable is not optimized to work with it. The two others implementations seem to be quite independant from the group size, and quite similar in there performances.

