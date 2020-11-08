package spark;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.lang.reflect.Array;
import java.util.*;


public class HashGroupBySpark {

    public final JavaRDD<ArrayList<String>> file;
    public final JavaSparkContext sc;
    private final int col_by;
    private final int agg_on;


    public HashGroupBySpark(String file_path,int nb_threads,int col_by, int agg_on){
        this.col_by = col_by;
        this.agg_on = agg_on;
        long t5 = System.nanoTime();
        SparkConf conf = new SparkConf().setAppName("spark.HashGroupBy").setMaster("local[*]");
        conf.set("spark.testing.memory", "471859200");
        conf.registerKryoClasses(new Class<?>[]{CustomHashMap.class, CustomHashMap.HashMapEntry.class});
        this.sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        sc.setLogLevel("INFO");
        this.file = sc.textFile(file_path,nb_threads)
                .map((line) -> new ArrayList<>(Arrays.asList(line.split(";"))));
               // .filter(line -> (line.size() > 1))
                //.filter(line -> !(line.get(0).equals("id")));


    }


    private static List<String> concat(List<String> array, String element){
        /* Just a small function to concatenate a String to an array of String and return the result
         */
        array.add(element);
        return array;
    }

    public static class Merge implements Function2<CustomHashMap,CustomHashMap,CustomHashMap> {

        private final Aggregation agg;

        public Merge(Aggregation agg){
            this.agg = agg;
        }

        @Override
        public CustomHashMap call(CustomHashMap t1, CustomHashMap t2) {
            return agg.mergeTables(t1,t2);
        }
    }

    public static class HashPartition implements FlatMapFunction<Iterator<ArrayList<String>>,CustomHashMap>
    {
        private final Aggregation agg;


        public HashPartition(Aggregation agg){
            this.agg = agg;

        }
        public Iterator<CustomHashMap> call(Iterator<ArrayList<String>> records_raw_it) {
        /*
        Takes a list of record and perform group-by on the partition
         */
            List<ArrayList<String>> records_raw = IteratorUtils.toList(records_raw_it);
            Record[] records = Record.fromArray(records_raw);
            /*System.out.println("BEFORE");
            for (Record record : records) {
                System.out.println(Arrays.toString(record.data));
            }
            */

            HashGroupBy grp = new HashGroupBy(agg.getCol_by(), agg.getAgg_on(), this.agg);
            CustomHashMap[] result = new CustomHashMap[]{grp.apply(records)};
            return Arrays.stream(result).iterator();

        }


    }

    public CustomHashMap apply(){
        Aggregation agg_spark = new CountAggregation(this.agg_on,this.col_by);
        HashPartition grouBy = new HashPartition(agg_spark);
        CustomHashMap output =this.file.mapPartitions(grouBy).reduce(new Merge(agg_spark));
        sc.stop();
        return output;
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
        HashGroupBySpark groupBySpark = new HashGroupBySpark("src/main/resources/data_test1.csv",4,1,0);
        // Make the group-by on every partition (mapPartitions) then merge everything (reduce)
        CustomHashMap res = groupBySpark.apply();
        //Print output
        Record[] values_res = res.values();
        for(Record record:values_res){
            System.out.println(Arrays.toString(record.data));
        }

    }
}
