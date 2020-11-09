package org.ulysse.project_maven;

import org.apache.spark.*;
import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

// This class implements the Nested Loops algorithm with early aggregation for a Spark multithreaded architecture
public class NestedLoopsSpark {
	
	// These attributes are static, otherwise they can't be used in map functions
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
		
		String pattern = Pattern.quote(System.getProperty("file.separator"));
    	String[] splittedFileName = inputName.split(pattern);
    	String fileNameInput = splittedFileName[splittedFileName.length -1];
		
		output = new WriterFile(MainForTest.TMP_PATH + "output_" + fileNameInput.substring(0, fileNameInput.length() - 4) + MainForTest.FILE_TYPE);
		
		conf.setAppName("NestLoopsSpark").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
	}
	
    public void apply() throws IOException {
    	
    	// Load the CSV file in a JavaRDD (e.g in memory)
    	JavaRDD<String> csvFile = sc.textFile(this.inputName, this.nbThreads);
    	String header = csvFile.first();
    	JavaRDD<String[]> csvLines = csvFile.filter(row -> row != header).map(line -> line.split(";"));
    	
    	// Apply the nested loops algorithm with early aggregation to each partition of the JavaRDD
    	JavaRDD<Map.Entry<String, Integer>> hashtableRDD = csvLines.mapPartitions(x -> earlyAggregation(x).iterator());
    	
    	/* Gather the partitions and merge the intermediary output hashtables.
    	 * Note that we haven't used a reduce function. To replace it we have gathered the partitions into one partition and then applied
    	 * the mapPartition function to all the entries.
    	 * We have done this in order to only use nested loops algorithms for the GROUP BY and avoid the reduceByKey function provided by Spark.
    	 */
    	JavaRDD<Map.Entry<String, Integer>> mergedOutputsRDD = hashtableRDD.coalesce(1).mapPartitions(x -> mergeIntermediaryOutputs(x).iterator());
    	
    	// Write the result of the GROUP BY on an output CSV file
    	JavaRDD<String> resultRDD = mergedOutputsRDD.map(obj -> obj.getKey() + ";" + Integer.toString(obj.getValue()));
    	resultRDD.foreach((String line) -> {
    		output.writeLine(line);
    	});    	
    	
    	output.closeFile();
    	this.sc.stop();
    }
    
    // Map function applying the nested loops algorithm to merge the intermediary output hashtables produced by each partition.
    public static Iterable<Map.Entry<String, Integer>> mergeIntermediaryOutputs(Iterator<Map.Entry<String, Integer>> x) {
    	Hashtable<String, Integer> outputTable = new Hashtable<String, Integer>();
 		while(x.hasNext()) {
 			Map.Entry<String, Integer> obj = x.next();
 	    	if (outputTable.containsKey(obj.getKey())) {
 	    		outputTable.put(obj.getKey(), outputTable.get(obj.getKey()) + obj.getValue());
 			} else {
 				outputTable.put(obj.getKey(), obj.getValue());
 			}
 		}
 		Set<Map.Entry<String, Integer>> iterableOutputTable = outputTable.entrySet();
     	return iterableOutputTable;
    }
    
    /* Map function applying the nested loops algorithm to the partition given.
     * Return a hashtable containing the result of the aggregation of the partition as an iterable object.
     */
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