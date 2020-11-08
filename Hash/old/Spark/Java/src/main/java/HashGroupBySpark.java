
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.*;


public class HashGroupBySpark {

    private static List<String> concat(List<String> array, String element){
        /* Just a small function to concatenate a String to an array of String and return the result
         */
        array.add(element);
        return array;
    }

    private static class Merge implements Function2<Record[], Record[], Record[]> {


        @Override
        public Record[] call(Record[] v1, Record[] v2) {
            Record[] v3 = (Record[]) ArrayUtils.addAll(v1,v2);
            Aggregation agg = new SumAggregation();
            // the intermediate results are in the format
            // groupping_attribute;aggregation_value
            //example : student;45
            //          teacher;33
            //so we apply again HashGroupBy on this new simple table
            HashGroupBy grp = new HashGroupBy(0, 1, agg);
            return grp.apply(v3);
        }
    }

    private static class HashPartition implements FlatMapFunction<Iterator<List<String>>,Record[]>
    {
        private final Aggregation agg;
        private final int by;
        private final int agg_on;

        HashPartition(Aggregation agg, int by, int agg_on){
            this.agg = agg;
            this.by = by;
            this.agg_on = agg_on;
        }
        public Iterator<Record[]> call(Iterator<List<String>> records_raw_it) {
        /*
        Takes a list of record and perform group-by on the partition
         */
            List<List<String>> records_raw = IteratorUtils.toList(records_raw_it);
            Record[] records = Record.fromArray(records_raw);
            System.out.println("BEFORE");
            for (Record record : records) {
                System.out.println(Arrays.toString(record.data));
            }
            HashGroupBy grp = new HashGroupBy(this.by, this.agg_on, this.agg);
            Record[][] result = new Record[][]{grp.apply(records)};
            return Arrays.stream(result).iterator();

        }


    }



    public static void main(String[] args) {

        // Spark Configuration
        SparkConf conf = new SparkConf().setAppName("HashGroupBy").setMaster("local[*]");
        conf.set("spark.testing.memory", "471859200");
        JavaSparkContext sc = new JavaSparkContext(conf);



        // Spark read CSV and format it
        /*
        CSV FORMAT
        id;role;names;height
        1;Student;Nora;56
        ...
         */

        JavaRDD<List<String>> file = sc.textFile("src/main/resources/data_test1.csv")
                .map((line) -> new ArrayList<>(Arrays.asList(line.split(";"))))
                .filter(line -> (line.size() > 1))
                .filter(line -> !(line.get(0).equals("id")))
                .map(line -> concat(line,Integer.toString(line.get(1).hashCode()%10)));

        // Make the group-by on every partition (mapPartitions) then merge everything (reduce)
        Record[] output;
        Aggregation agg = new SumAggregation();
        HashPartition grouBy = new HashPartition(agg,1,3);
        output = file.mapPartitions(grouBy).reduce(new Merge());

        //Print output
        for(Record record:output){
            System.out.println(Arrays.toString(record.data));
        }

    }
}
