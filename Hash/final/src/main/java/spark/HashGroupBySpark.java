package spark;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.*;


public class HashGroupBySpark {

    public final JavaRDD<ArrayList<String>> file;

    public HashGroupBySpark(String file_path,int nb_threads){
        long t5 = System.nanoTime();
        SparkConf conf = new SparkConf().setAppName("spark.HashGroupBy").setMaster("local[*]");
        conf.set("spark.testing.memory", "471859200");
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));

        this.file = sc.textFile(file_path,nb_threads)
                .map((line) -> new ArrayList<>(Arrays.asList(line.split(";"))))
                .filter(line -> (line.size() > 1))
                .filter(line -> !(line.get(0).equals("id")));


    }


    private static List<String> concat(List<String> array, String element){
        /* Just a small function to concatenate a String to an array of String and return the result
         */
        array.add(element);
        return array;
    }

    public static class Merge implements Function2<Record[], Record[], Record[]> {


        @Override
        public Record[] call(Record[] v1, Record[] v2) {

            Record[] v3 = (Record[]) ArrayUtils.addAll(v1,v2);
            Aggregation agg = new SumAggregation();
            // the intermediate results are in the format
            // groupping_attribute;aggregation_value
            //example : student;45
            //          teacher;33
            //so we apply again spark.HashGroupBy on this new simple table
            HashGroupBy grp = new HashGroupBy(0, 1, agg);
            Record[] res = grp.apply(v3);

            return res;
        }
    }

    public static class HashPartition implements FlatMapFunction<Iterator<ArrayList<String>>,Record[]>
    {
        private final Aggregation agg;
        private final int by;
        private final int agg_on;

        public HashPartition(Aggregation agg, int by, int agg_on){
            this.agg = agg;
            this.by = by;
            this.agg_on = agg_on;
        }
        public Iterator<Record[]> call(Iterator<ArrayList<String>> records_raw_it) {
        /*
        Takes a list of record and perform group-by on the partition
         */
            List<ArrayList<String>> records_raw = IteratorUtils.toList(records_raw_it);
            Record[] records = Record.fromArray(records_raw);
            /*System.out.println("BEFORE");
            for (Record record : records) {
                System.out.println(Arrays.toString(record.data));
            }*/

            HashGroupBy grp = new HashGroupBy(this.by, this.agg_on, this.agg);
            Record[][] result = new Record[][]{grp.apply(records)};
            return Arrays.stream(result).iterator();

        }


    }



    public static void main(String[] args) {

        // Spark Configuration




        // Spark read CSV and format it
        /*
        CSV FORMAT
        id;role;names;height
        1;Student;Nora;56
        ...
         */
        HashGroupBySpark groupBySpark = new HashGroupBySpark("src/main/resources/data_test1.csv",4);

        // Make the group-by on every partition (mapPartitions) then merge everything (reduce)

        Aggregation agg = new CountAggregation();
        HashPartition grouBy = new HashPartition(agg,1,0);

        JavaRDD<Record[]> output = groupBySpark.file.mapPartitions(grouBy);
        output.reduce(new Merge());

        /*Print output
        for(Record record:output){
            System.out.println(Arrays.toString(record.data));
        }*/

    }
}
