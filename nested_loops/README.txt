Here is the directory of the nested loops with early aggregation algorithms for the GROUP BY.
This is an excerpt of the report which gives informations about how we have implemented the algorithm with 3 different architectures.

2.1. The 3 architectures

2.1.1 Single threading
As we have seen, the nested loops with early aggregation algorithm achieves a maximum of aggregations directly in memory and stores the other elements in an overflow file. Intuitively, the larger the size of available memory, the lower the number of I/Os, and the faster the program will perform. To analyze this phenomenon, we therefore “simulate” a memory of fixed size by a hash table whose size MEMORY_SIZE is a program variable. Thus, the program reads the records directly from the input file, performs aggregations on as many records as possible within the hash table, and stores the other records in an overflow file on disk. Once the program went through  all the input dataset, the content of the hash table is written to an output file on the disk, then the process is repeated taking the overflow file as the input file. In this way, we can precisely control the maximum size of the memory used. Temporary files such as the overflow file are stored in a tmp folder created by the program and are deleted at the end of the program.
More precisely, we have implemented two classes ReaderFile and WriterFile which are used respectively to read data from a file on the disk, and to write in such a file. Another class NestedLoopsSingleThread contains the implementation of the algorithm. As we will see, this class implements the Runnable interface to be instancied as a Thread object, because it will be used for multithreading.

2.1.2 Multithreading
The multithreaded version of the nested loops algorithm is implemented within the NestedLoopsMultiThread class. It consists of dividing the input file into several blocks, each stored in a new temporary file on the disk, then performing a nested loops with early aggregation on each block in different threads, and finally merging the output files produced by each thread into a single output file. In this implementation, we kept our logic of memory simulated by a hash table. Indeed, each thread created by an instance of the NestedLoopsMultiThread class executes an instance of NestedLoopsSingleThread with its own hash table, and its own temporary files stored on the disk. Once these nested loops are executed, the tmp folder contains all the output files produced by each thread. These files are then concatenated in order to produce a new temporary input file, containing a much smaller number of records, on which a last iteration of the nested loops algorithm is performed, but this time with the sum aggregation function, to add the values ​​of the aggregation functions of each group. This last iteration therefore produces the final output file stored in the tmp folder on the disk.

2.1.3. Multithreading with Spark
Using Spark for multithreading imposed a different method than what we had used before. In fact, in the previous multithreaded version, we imposed a maximum size for the memory used, and the program stored temporary files on the disk and read or wrote directly to them. With Spark, the input file must first be stored in a resilient distributed dataset (RDD), and the resulting output must then be written to a file. As a result, the execution time of this implementation is much greater than the execution time of the other nested loops algorithm.
More precisely, we first loaded the input CSV file in a JavaRDD object divided in the number of partitions chosen by the user. Then we map each partition with a function performing a nested loop algorithm directly in memory on each partition. After such a mapping our JavaRDD contains partitions corresponding to the results of the aggregation for each partition and we need to merge them to get the final output of the GROUP BY request.Instead of doing it with the reduceByKey function provided by Spark we chose to merge them using the nested loops algorithm. To do so we mapped the entire JavaRDD object with a function merging the identical groups by adding their counts. Finally we get a JavaRDD containing the result of our request and after some transformations we write it in a CSV file as for the other implementations.

2.2. Classical user and Test versions

Our implementation of the Nested Loops algorithm with early aggregation contains two classes having a main function to provide two possibilities for the user.

2.2.1. The classical user version
The classical user version can be launched thanks to the main function of the Main class and provides a user friendly interface where the user can :
Choose the CSV file he wants to GROUP BY in a windowed interface provided by a JFileChooser.
Choose the feature he wants to GROUP BY.
Choose if he wants to use Spark or classical Java threads.
Choose the number of threads he wants to use.
After all these choices the algorithm runs and a CSV file containing the result of the request is created in a tmp directory in the parent directory of the project.

2.2.2. The test version
The test version is available by using the main function of the MainForTest class. This version is useful to execute our algorithm on every CSV file of a given directory. It executes the single threaded, multi-threaded and multi-threaded with Spark implementations several times on all the files and builds a CSV file containing the mean of execution times for each architecture for each file. We compute the mean of all execution times for each file for each architecture because since our execution times are very small they can vary. The user has to provide two arguments to this function :
First argument : the path toward the test directory.
Second argument : the position of the feature on which the user wants to apply the GROUP BY request (starting at 0).
Third argument : the number of repetitions of each algorithm for each architecture.
The times of execution are given in milliseconds in the file result.csv which can be found in the tmp directory with one CSV file containing the output of the GROUP BY request for each file. Note that this version has been useful to provide data for the graphs found in the comparison part of the report.
