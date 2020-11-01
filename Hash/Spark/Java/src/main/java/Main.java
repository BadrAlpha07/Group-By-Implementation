import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Main {

    private static List<String> concat(List<String> array, String element){
        /* Just a small function to concatenate a String to an array of String and return the result

         */

        array.add(element);
        return array;
    }

    private static Object test(Object file, int by, String agg, int agg_on){
        /*
        Not implemented yet
         */
        return null;
    }

    private static Record[] hashPartition(List<List<String>> iterator){
        /*
        Takes a list of record and perform group-by on the partition
         */
        Record[] records = Record.fromArray(iterator);
        Aggregation agg = new CountAggregation(3,1);
        HashGroupBy grp =  new HashGroupBy(1,3,1,0,agg);
        return grp.apply(records);
    }



    public static void main(String[] args) {

        // Spark Configuration
        SparkConf conf = new SparkConf().setAppName("HashGroupBy").setMaster("local[*]");
        conf.set("spark.testing.memory", "471859200");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Spark read CSV and format it
        JavaRDD<List<String>> file = sc.textFile("src/main/resources/data_test1.csv")
            .map((line) ->  new ArrayList<String>(Arrays.asList(line.split(";"))))
            .filter(line -> (line.size() > 1))
            .filter(line -> !(line.get(0).equals("id")))
            .map(line -> concat(line,Integer.toString(line.get(1).hashCode()%10)));

        // We collect the result and try to group by
        List<List<String>> aggregate = file.collect();
        Record[] output = hashPartition(aggregate);
        for(Record record:output){
            System.out.println(Arrays.toString(record.data));
        }
        ;
    }
}
