package org.ulysse.project_maven;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import javax.swing.JFileChooser;

public class Main {
	
	// This variable models the memory size of our system
	public static final int MEMORY_SIZE = 300;
	
	public static final String TMP_PATH = "./tmp/";
	public static final String FILE_TYPE = ".csv";
	
	public static void main(String args[]) {
		
		int nbProcessors = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of processors available: " + Integer.toString(nbProcessors));
        
		JFileChooser chooser = new JFileChooser();
		File workingDirectory = new File(System.getProperty("user.dir"));
		chooser.setCurrentDirectory(workingDirectory);
        int status = chooser.showOpenDialog(null);
        if (status == JFileChooser.APPROVE_OPTION) {
            File file = chooser.getSelectedFile();
            if (file == null) {
                return;
            }

            // Get the file chosen for the GROUP BY
            String inputName = chooser.getSelectedFile().getAbsolutePath();
            File temp = new File(TMP_PATH);
    		temp.mkdirs();
            
            ReaderFile input = new ReaderFile(inputName);
            
            String line = input.readLine();
    		// Here we print the first row to see our features names
    	    System.out.println(line);
    		String[] splittedLine = line.split(";");
    		int maxPosition = splittedLine.length ;
    	    input.closeFile();
     
    	    // Get the feature chosen for the GROUP BY
    	    Scanner sc = new Scanner(System.in);        
    	    System.out.println("Enter the position of the column you want to group by:");
    	    int positionGroup = sc.nextInt();
    	    
    	    while(positionGroup<0 || positionGroup >= maxPosition) {
    	    	System.out.println("please try again!");
    	        positionGroup = sc.nextInt();
    	    }
    	    
    	    // Get the parameter to know if we use spark or not
    	    System.out.println("Do you want to use Spark (1 if Yes, 0 if No):");
    	    int spark = sc.nextInt();
    	    
    	    while(spark != 0 && spark != 1) {
    	    	System.out.println("please try again!");
    	        spark = sc.nextInt();
    	    }
    	    
    	    // Get the number of threads chosen
    	    System.out.println("Enter the number of threads you want:");
    	    int nbThreads = sc.nextInt();
    	    
    	    while(nbThreads < 1) {
    	    	System.out.println("please try again!");
    	        nbThreads = sc.nextInt();
    	    }
    	    sc.close();
    	    
    	    if(spark == 0) {        	    
        	    long t1 = System.nanoTime();
        	    
        	    NestedLoopsMultiThread nestedLoops = new NestedLoopsMultiThread(inputName, positionGroup, nbThreads);
        	    nestedLoops.apply();
        	    
                long t2 = System.nanoTime();
        	    // Measure the execution time and print it
                long timing = (t2-t1)/(1000000*10);
                System.out.format("Processing time: %d ms\n", timing);
    	    } 
    	    else {
    	    	NestedLoopsSpark nestedLoops = new NestedLoopsSpark(inputName, positionGroup, nbThreads);
    	    	
    	    	long t1 = System.nanoTime();
    	    	
    	    	try {
					nestedLoops.apply();
				} catch (IOException e) {
					e.printStackTrace();
				}
    	    	
    	    	long t2 = System.nanoTime();
                long timing = (t2-t1)/(1000000*10);
                System.out.format("Processing time: %d ms\n", timing);
    	    }
        }		
	}
}
