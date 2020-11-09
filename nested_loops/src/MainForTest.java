package org.ulysse.project_maven;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Pattern;

import javax.swing.JFileChooser;

public class MainForTest {
	
	// This variable models the memory size of our system
	public static final int MEMORY_SIZE = 100000;
	
	public static final String TMP_PATH = "./tmp/";
	public static final String FILE_TYPE = ".csv";
	
	/* Give the directory path as first argument.
	 * Give the position of the feature you want to group by as second argument.
	 * Give the number of runs you want for each algorithm for each file, then the results are the mean of all these runs.
	 */
	public static void main(String args[]) {
		
		int nbProcessors = Runtime.getRuntime().availableProcessors();
		String dirPath = args[0];
		int positionGroup = Integer.parseInt(args[1]);
		int repetitions = Integer.parseInt(args[2]);
		
        File temp = new File(TMP_PATH);
        temp.mkdirs();
    	
        WriterFile result = new WriterFile(TMP_PATH + "result.csv");
        result.writeLine("File Name; Single; Multi; Spark");
        File[] files = new File(dirPath).listFiles();
        
        // the following 6 lines are only here in order to initialize Spark before tracking the execution times
        NestedLoopsSpark testSpark = new NestedLoopsSpark(files[0].getAbsolutePath(), positionGroup, nbProcessors);
    	try {
			testSpark.apply();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
        // loop over the directory
        for(File file : files) {
        	
        	long timingSingle = 0;
        	long timingMulti = 0;
        	long timingSpark = 0;
        	
        	String fileName = file.getName();
        	String times = fileName;
        	
        	for(int i = 0; i < repetitions; i++) {
            	
            	long t1 = System.nanoTime();
        	    NestedLoopsMultiThread nestedLoopsSingle = new NestedLoopsMultiThread(file.getAbsolutePath(), positionGroup, 1);
        	    nestedLoopsSingle.apply();
                long t2 = System.nanoTime();
                timingSingle += (t2-t1)/(1000000*10);
                
                long t3 = System.nanoTime();
        	    NestedLoopsMultiThread nestedLoopsMulti = new NestedLoopsMultiThread(file.getAbsolutePath(), positionGroup, nbProcessors);
        	    nestedLoopsMulti.apply();
                long t4 = System.nanoTime();
                timingMulti += (t4-t3)/(1000000*10);
                
                NestedLoopsSpark nestedLoopsSpark = new NestedLoopsSpark(file.getAbsolutePath(), positionGroup, nbProcessors);
    	    	long t5 = System.nanoTime();
    	    	try {
    				nestedLoopsSpark.apply();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    	    	
    	    	long t6 = System.nanoTime();
                timingSpark += (t6-t5)/(1000000*10);
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
