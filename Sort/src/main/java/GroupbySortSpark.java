

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Iterator;

import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;


public class GroupbySortSpark {
    
    
    public static Iterable<Map.Entry<Integer , Integer>> GroupbySortSpark(Iterator<String[]> x,int col) {

                SingleThreaded grps=new SingleThreaded();
                Map<Integer, Integer> arr2 = Collections.synchronizedMap(new LinkedHashMap<Integer, Integer>());
                Set<Map.Entry<Integer, Integer>> iterableOutputTable = arr2.entrySet();
                iterableOutputTable=(Set<Map.Entry<Integer, Integer>>) grps.groupbySort(x,col);
     	return iterableOutputTable;
     }
    // Merge data from all partitions
    
        public static Iterable<Map.Entry<Integer , Integer>>  mergeIntermediaryOutputs(Iterator<Map.Entry<Integer , Integer>> x) {
    	Map<Integer, Integer> outputTable = Collections.synchronizedMap(new LinkedHashMap<Integer, Integer>());
 		while(x.hasNext()) {
 			Map.Entry<Integer, Integer> obj = x.next();
 	    	if (outputTable.containsKey(obj.getKey())) {
 	    		outputTable.put(obj.getKey(), outputTable.get(obj.getKey()) + obj.getValue());
 			} else {
 				outputTable.put(obj.getKey(), obj.getValue());
 			}
 		}
 		Set<Map.Entry<Integer, Integer>> iterableOutputTable = outputTable.entrySet();
     	return iterableOutputTable;
     	
     }

        // Load the CSV file in a JavaRDD
        public Map<Integer , Integer> Spark_grouby(String path, int nbThreads,int col,JavaSparkContext sc) throws FileNotFoundException{
    
        JavaRDD<String> File = sc.textFile(path);
        String header = File.first();
    	JavaRDD<String[]> Lines = File.filter(row -> !row.equals(header)).map(line -> line.split(";"));
        JavaRDD<Map.Entry<Integer , Integer>> hashtable = Lines.mapPartitions(x -> GroupbySortSpark(x,col).iterator());
        JavaRDD<Map.Entry<Integer , Integer>> merged = hashtable.coalesce(1).mapPartitions(x -> mergeIntermediaryOutputs(x).iterator());
        JavaRDD<String> Output = merged.map(obj -> obj.getKey() + ";" + Integer.toString(obj.getValue()));
        Output.cache();
        //List<String> matrix=Output.collect();
        
 
       Map<Integer , Integer> arr2 = Collections.synchronizedMap(new LinkedHashMap<Integer , Integer>());
        
        for (String line:Output.collect()) {
            String[] spt=line.split(";");
            arr2.put(Integer.parseInt(spt[0]),Integer.parseInt(spt[1]));
        }
        Export_csv save = new Export_csv();
        save.export_csv(arr2,"result.csv");
         
        return arr2;
        
}
}