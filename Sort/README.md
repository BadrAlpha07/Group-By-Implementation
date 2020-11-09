# Group By sort
In this section, we implement the group by sorting which consists of first sorting data, then aggregate it. This approach help to identify easily the groups since they are ordered one after the other. We choose to perform 3 sorting algorithms, Selection Sort, Merge Sort and Heap Sort. We gather all java classes into one main classe that contains the user interface.
The user can choose at first the CSV file, secondly the column name that contains groups and finally choose the implementation mode, the results are written in a CSV file

## Architecture
The main code used to generate the results files is [Main_Test.java](Sort/src/main/java/Main_Test.java).
We have done 3 implemtentations :
 - Single-Threaded Group By Sort, that can be found [there](Sort/src/main/java/SingleThreaded.java).
 - Multi-Threaded Group By Sort, that can be found  [there](Sort/src/main/java/MultiThreaded.java).
 - Spark distributed Group By Sort, that can be found [there](Sort/src/main/java/GroupbySortSpark.java).
The class [MainSortGroup.java] used to implement the interface [there](Sort/src/main/java/MainSortGroup.java).


## Implementation details
### Single Threaded
The first implementation mode is the single thread, it assumes that the execution of instructions is done in a single sequence. In other words, processes all the data in just one thread, 

### Multithreaded Implementation
In multi-threads Java, we created  Groupyby_Thread class that implements the Callable classe which an improved version of Runnable class which allows the use of thread under JAVA with the possibility of returning values such as string matrix in our case. The idea behind multi-threading in JAVA is to split task between threads that work on small partitions of data so in a faster way and output a sorted aggregated matrix of 2 columns: Id of the group and its counting. So, the next step is to merge the outputs by summing the counting of the groups that may exist in other partitions, so to do that we use Merge_parts function that concatenates the output of two threads and apply grouping by sorting again but using Sum as aggregation function (and here where we use the parameter p that we mentioned in heap sort class’s description above). These steps are organized in global function GroupingMulti.

### Spark Implementation
Spark is an open-source parallel data processing engine for performing large-scale analysis through clustered machines which allows users to take advantage of multicore systems without imposing the overhead of creating multiple processes and providing direct access to a common address space. The use of fast performing Resilient Distributed Datasets (RDDs) in Spark allows to store temporary the input file in a number of partitions specified by the user in a JavaRDD object, then perform the sorting algorithm to order the data and aggregate it in each partition using the mapping. Finally, the partitions in the JavaRDD are merged by summing the count of each identical group from all the partitions. The RDD structure makes the execution time of the grouping implementation very low than single and multithread implementation. However, Transforming the stored data from JavaRDD to a CSV file takes more time. 

### User interface
In order to facilitate the utilization of the algorithm, we gather all classes in one interface that allows to user to select the desired csv file.
After getting the path, the user can choose the column name from the csv file picked previously that contains groups and the implementation mode (single thread, multi-thread and Spark). 
