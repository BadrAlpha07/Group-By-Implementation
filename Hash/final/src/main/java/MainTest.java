

import org.apache.spark.api.java.JavaRDD;
import raw_java.*;
import spark.HashGroupBy;
import spark.HashGroupBySpark;
import utils.WriterFile;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class MainTest {

    // This variable models the memory size of our system
    public static final int MEMORY_SIZE = 100000;

    public static final String TMP_PATH = "./tmp/";
    public static final String FILE_TYPE = ".csv";

    /* Give the directory path as first argument.
     * Give the position of the feature you want to group by as second argument.
     * Give the number of runs you want for each algorithm for each file, then the results are the mean of all these runs.
     */
    public static void main(String args[]) {
        System.out.println(Arrays.toString(args));
        int nbProcessors = Runtime.getRuntime().availableProcessors();
        String dirPath = args[0];
        int positionGroup = Integer.parseInt(args[1]);
        int repetitions = Integer.parseInt(args[2]);
        int nbThreads =4;
        File temp = new File(TMP_PATH);
        temp.mkdirs();

        WriterFile result = new WriterFile(TMP_PATH + "result.csv");
        result.writeLine("File Name; Single; Multi; Spark");
        File[] files = new File(dirPath).listFiles();

        // the following 6 lines are only here in order to initialize Spark before tracking the execution times
        HashGroupBySpark groupBySparkTest = new HashGroupBySpark(files[0].getAbsolutePath(),nbThreads);
        spark.Aggregation agg_spark_test = new spark.CountAggregation();
        HashGroupBySpark.HashPartition grouBy_test = new HashGroupBySpark.HashPartition(agg_spark_test,positionGroup,0);
        spark.Record[] output_spark_test = groupBySparkTest.file.mapPartitions(grouBy_test).reduce(new HashGroupBySpark.Merge());
        output_spark_test = null;

        // loop over the directory
        for(File file : files) {

            long timingSingle = 0;
            long timingMulti = 0;
            long timingSpark = 0;

            String times = file.getName();

            for(int i = 0; i < repetitions; i++) {

                long t1 = System.nanoTime();

                Aggregation agg = new CountAggregation(0,positionGroup);
                HashGroupBySingle single_grp = new HashGroupBySingle(positionGroup,0, 1, 0, agg);
                Record[] output = single_grp.apply(file.getAbsolutePath());
                output = null;
                long t2 = System.nanoTime();
                timingSingle += (t2-t1)/(1000000*10);

                long t3 = System.nanoTime();
                agg = new CountAggregation(0,positionGroup);
                PartitioningHashGroupBy partitioning_grp = new PartitioningHashGroupBy(positionGroup,0, 1, 0, agg, 4);
                output = partitioning_grp.apply(file.getAbsolutePath());
                output = null;
                long thread_creation_cost = partitioning_grp.thread_creation_time;
                long t4 = System.nanoTime();
                timingMulti += (t4-t3)/(1000000*10);



                HashGroupBySpark groupBySpark = new HashGroupBySpark(file.getAbsolutePath(),4);
                spark.Aggregation agg_spark = new spark.CountAggregation();
                HashGroupBySpark.HashPartition grouBy = new HashGroupBySpark.HashPartition(agg_spark,1,0);
                JavaRDD<spark.Record[]> output_spark = groupBySpark.file.mapPartitions(grouBy);
                long t5 = System.nanoTime();
                output_spark.reduce(new HashGroupBySpark.Merge());
                output_spark = null;
                long t6 = System.nanoTime();
                timingSpark += (t6-t5)/(1000000*10);
                System.out.println(timingSpark);

            }

            timingSingle /= repetitions;
            timingMulti /= repetitions;
            timingSpark /= repetitions;
            times = times + "; " + Long.toString(timingSingle)+ "; " + Long.toString(timingMulti) + "; " + Long.toString(timingSpark);

            result.writeLine(times);
            System.out.println("File Done");
        }
        System.out.println("Directory Done");
        result.closeFile();
    }
}
