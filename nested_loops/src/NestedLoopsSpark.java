package org.ulysse.project_maven;

import org.apache.spark.*;
import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

public class NestedLoopsSpark {
	
	private static int positionGroupStat = 0;
	private static WriterFile output = null;
	private String inputName = "";
	private int nbThreads = 0;
	
	private SparkConf conf = new SparkConf();
	private JavaSparkContext sc = null;
	
	public NestedLoopsSpark(String inputName, int positionGroup, int nbThreads) {
		positionGroupStat = positionGroup;
		this.nbThreads = nbThreads;
		this.inputName = inputName;
		output = new WriterFile(Main.TMP_PATH + "output" + Main.FILE_TYPE);
		
		conf.setAppName("NestLoopsSpark").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
	}
	
    public void apply() throws IOException {
    	
//		Load the CSV file in a JavaRDD (e.g in memory)
    	JavaRDD<String[]> csvLines = sc.textFile(this.inputName, this.nbThreads).map(line -> line.split(";"));
        
//    	Apply the nested loops algorithm with early aggregation to each partition of the JavaRDD
    	JavaRDD<Map.Entry<String, Integer>> hashtableRDD = csvLines.mapPartitions(x -> earlyAggregation(x).iterator());
        
//    	Transform the JavaRDD containing the output HashTable of each partition to a JavaPairRDD and merge these HashTables
    	JavaPairRDD<String, Integer> keyValuePairs = hashtableRDD.mapToPair(obj -> new Tuple2(obj.getKey(), obj.getValue()));
    	JavaPairRDD<String, Integer> partitionsMerged = keyValuePairs.reduceByKey((a,b) -> (a + b));
    	
//    	Write the result of the GROUP BY on an output CSV file
    	JavaRDD<String> resultRDD = partitionsMerged.map(obj -> obj._1 + ";" + Integer.toString(obj._2));
    	resultRDD.foreach((String line) -> {
    		output.writeLine(line);
    	});    	
    	
    	output.closeFile();
    	this.sc.stop();
    }
    
//  map function making the early aggregation returning an iterable object 
 	public static Iterable<Map.Entry<String, Integer>> earlyAggregation(Iterator<String[]> x) {
 		Hashtable<String, Integer> outputTable = new Hashtable<String, Integer>();
 		while(x.hasNext()) {
 			String[] obj = x.next();
 	    	if (outputTable.containsKey(obj[positionGroupStat])) {
 	    		outputTable.put(obj[positionGroupStat], outputTable.get(obj[positionGroupStat]) + 1);
 			} else {
 				outputTable.put(obj[positionGroupStat], 1);
 			}
 		}
 		Set<Map.Entry<String, Integer>> iterableOutputTable = outputTable.entrySet();
     	return iterableOutputTable;
     }
}