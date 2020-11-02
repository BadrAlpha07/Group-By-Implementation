
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.*;


public class Main {

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
            HashGroupBy grp = new HashGroupBy(0, 1, agg);
            return grp.apply(v3);
        }
    }

    private static class HashPartition implements FlatMapFunction<Iterator<List<String>>,Record[]>
    {
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

        Aggregation agg = new CountAggregation();
        HashGroupBy grp = new HashGroupBy(1, 3, agg);
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
        JavaRDD<List<String>> file = sc.textFile("src/main/resources/data_test1.csv")
            .map((line) -> new ArrayList<>(Arrays.asList(line.split(";"))))
            .filter(line -> (line.size() > 1))
            .filter(line -> !(line.get(0).equals("id")))
            .map(line -> concat(line,Integer.toString(line.get(1).hashCode()%10)));

        // We collect the result and try to group by
        Record[] output;
        output = file.mapPartitions(new HashPartition()).reduce(new Merge());
        //Record[] output = hashPartition(aggregate);
        for(Record record:output){
            System.out.println(Arrays.toString(record.data));
        }

    }
}
