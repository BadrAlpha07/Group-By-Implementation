package org.ulysse.project_maven;

import java.io.File;
import java.util.ArrayList;

/* This class implements the Nested Loops algorithm with early aggregation for a multithreaded architecture.
 * In order to create multiple threads we instantiate several NestedLoopsSingleThread objects, each having its
 * own partition piece of the initial input file.
 * To run the Nested Loops algorithm on a single thread we still use this class but with a parameter nbThreads equal to 1.
 */
public class NestedLoopsMultiThread {
	
	private int positionGroup;
	private int nbThreads;
	
	private WriterFile tempOutput = null;
	private String inputName = "";
	
	public NestedLoopsMultiThread(String inputName, int positionGroup, int nbThreads) {
		this.positionGroup = positionGroup;
		this.nbThreads = nbThreads;
		this.tempOutput = new WriterFile(Main.TMP_PATH + "temporaryOutput" + Main.FILE_TYPE);
		this.inputName = inputName;
	}
	
	public void apply() {
		
		this.splitInput();
		String beginInputName = this.inputName.substring(0, this.inputName.length() - 4);
		
		// create the threads
		ArrayList<Thread> threads = new ArrayList<>();
		
        for(int i = 1; i <= nbThreads; i++) {
        	NestedLoopsSingleThread nestedLoops = new NestedLoopsSingleThread(beginInputName, i, positionGroup);
        	Thread thread = new Thread(nestedLoops);
            thread.start();
            threads.add(thread);
        }
        
        for (Thread t : threads) {
            try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
        
        // concatenate the outputs of the threads
        this.concatenateOutputs();
        
        /* Transform the concatenated outputs into the final output file by gathering the couples having the same group name
         * by summing their counts. We have done it thanks to a GROUP BY with SUM as the aggregate function.
         */
        NestedLoopsSingleThread nestedLoops = new NestedLoopsSingleThread(tempOutput.getFileName(), 0, 0);
        Thread thread = new Thread(nestedLoops);
        thread.start();
        
        try {
			thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        // delete temporary files
        for(int i = 1; i <= nbThreads; ++i) {
        	File inp = new File(beginInputName + Integer.toString(i) + Main.FILE_TYPE);
        	inp.delete();
        }
        File tmpOut = new File(this.tempOutput.getFileName());
        tmpOut.delete();
	}
	
	/* This function creates a partition of the initial input file.
	 * To do so it divides the initial CSV file into several CSV files, each containing a
	 * portion of the initial file.
	 */
	public void splitInput() {
		ReaderFile input = new ReaderFile(this.inputName);
		int recordsNumber = input.getRecordsNumber();
		input.readLine();
		String line = input.readLine();
		String nameBlockInput = this.inputName.substring(0, this.inputName.length() - 4);
		int count = 0;
		int numberRecordsBlock = recordsNumber / this.nbThreads;
		
		WriterFile[] blockInputs = new WriterFile[this.nbThreads];
		for(int i=1; i <= this.nbThreads; i++) {
			blockInputs[i-1] = new WriterFile(nameBlockInput + Integer.toString(i) + Main.FILE_TYPE);
		}
		
		while(line != null) {
			int blockNumber = count / numberRecordsBlock;
			if(blockNumber < this.nbThreads) {
				blockInputs[blockNumber].writeLine(line);
			} else {
				blockInputs[blockNumber - 1].writeLine(line);
			}
			count++;
			line = input.readLine();
		}
		
		for(int i=1; i <= nbThreads; i++) {
			blockInputs[i-1].closeFile();
		}
		
		input.closeFile();
	}
	
	// This function concatenates the outputs files created by the multiple threads in one CSV file tempOutput.csv
	public void concatenateOutputs() {
		for(int i=1; i <= this.nbThreads; i++) {
			ReaderFile outputThreadI = new ReaderFile(Main.TMP_PATH + "output" + Integer.toString(i) + Main.FILE_TYPE);
			String line = outputThreadI.readLine();
			
			while(line != null) {
				tempOutput.writeLine(line);
				line = outputThreadI.readLine();
			}
			outputThreadI.deleteFile();
		}
		tempOutput.closeFile();
	}

}
