package org.ulysse.project_maven;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Pattern;

import javax.swing.JFileChooser;

public class MainForTest {
	
	// This variable models the memory size of our system
	public static final int MEMORY_SIZE = 300;
	
	public static final String TMP_PATH = "./tmp/";
	public static final String FILE_TYPE = ".csv";
	
	// give the directory path as first argument and the position of the feature you want to group by as second argument
	public static void main(String args[]) {
		
		int nbProcessors = Runtime.getRuntime().availableProcessors();
		// String dirPath = "C:\\Users\\udema\\eclipse-workspace\\project-maven\\dataTest\\";
		String dirPath = args[0];
		// int positionGroup = 0;
		int positionGroup = Integer.parseInt(args[1]);
		
        File temp = new File(TMP_PATH);
        temp.mkdirs();
    	
        WriterFile result = new WriterFile(TMP_PATH + "result.csv");
        result.writeLine("File Name; Single; Multi; Spark");
        File[] files = new File(dirPath).listFiles();
        for(File file : files) {
        	String fileName = file.getName();
        	String times = fileName;
        	
        	long t1 = System.nanoTime();
    	    NestedLoopsMultiThread nestedLoopsSingle = new NestedLoopsMultiThread(file.getAbsolutePath(), positionGroup, 1);
    	    nestedLoopsSingle.apply();
            long t2 = System.nanoTime();
            long timing = (t2-t1)/(1000000*10);
            times = times + "; " + Long.toString(timing);
            
            long t3 = System.nanoTime();
    	    NestedLoopsMultiThread nestedLoopsMulti = new NestedLoopsMultiThread(file.getAbsolutePath(), positionGroup, nbProcessors);
    	    nestedLoopsMulti.apply();
            long t4 = System.nanoTime();
            long timing2 = (t4-t3)/(1000000*10);
            times = times + "; " + Long.toString(timing2);
            
            NestedLoopsSpark nestedLoopsSpark = new NestedLoopsSpark(file.getAbsolutePath(), positionGroup, nbProcessors);
	    	long t5 = System.nanoTime();
	    	try {
				nestedLoopsSpark.apply();
			} catch (IOException e) {
				e.printStackTrace();
			}
	    	
	    	long t6 = System.nanoTime();
            long timing3 = (t6-t5)/(1000000*10);
            times = times + "; " + Long.toString(timing3);
            
            result.writeLine(times);
        }
        result.closeFile();        
        }		
	}
