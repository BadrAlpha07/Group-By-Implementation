package org.ulysse.project_maven;

import java.util.*;
import java.util.Map.Entry;
import java.io.File;
	
//This class implements the Nested Loops algorithm on a single thread
public class NestedLoopsSingleThread implements Runnable {

	// Hashtable simulating the memory, with a maximum size of MEMORY_SIZE
	private Hashtable<String, Integer> outputTable = new Hashtable<String, Integer>();
	
	private String fileNameTemporary = "";
	private String fileNameOverflow = "";
	private int threadNumber = 0;
	
	// Overflow file stores items that do not fit in memory
	private WriterFile overflowFile = null;
	private WriterFile outputFile = null;
	private ReaderFile inputFile = null;
	
	// position of the feature we want to group by
	private int positionGroup = 0;
	private boolean isTemporaryInput = false;
	
	
	// Constructor 
	public NestedLoopsSingleThread(String beginInputName, int threadNumber, int position) {
		this.positionGroup = position ;
		this.threadNumber = threadNumber;
		this.fileNameOverflow = MainForTest.TMP_PATH + "overflow" + Integer.toString(threadNumber) + MainForTest.FILE_TYPE;
		this.fileNameTemporary = MainForTest.TMP_PATH + "temporary" + Integer.toString(threadNumber) + MainForTest.FILE_TYPE;
		
		/* If the parameter threadNumber is equal to 0 it means that it is the thread for the transformation of the concatenated outputs into
		 * the final output thanks to the GROUP BY with SUM as aggregation function.
		 */
		if(threadNumber == 0) {
			this.outputFile = new WriterFile(MainForTest.TMP_PATH + "output" + MainForTest.FILE_TYPE);
			this.inputFile = new ReaderFile(beginInputName);
		} else {
			this.outputFile = new WriterFile(MainForTest.TMP_PATH + "output" + Integer.toString(threadNumber) + MainForTest.FILE_TYPE);
			this.inputFile = new ReaderFile(beginInputName + Integer.toString(threadNumber) + MainForTest.FILE_TYPE);
		}
		
		this.overflowFile = new WriterFile(fileNameOverflow);
	}
	
	public void run() {
	    
	    boolean check = true;
	    while (check) {
	    	check = this.processInputItem();
	    }
	    
	    this.outputFile.closeFile();
	    this.inputFile.closeFile();
	    
	    if(this.getIsTemporaryInput()) {
	    	this.deleteTemporaryFile();
	    }
	}
	
	
	/* Process an item of the input file.
	 * Return true if we are not at the end of the input file or if the overflow file is not empty.
	 * Return false if we are at the end of the input file and the overflow is empty
	 */
	public boolean processInputItem() {
		String line = inputFile.readLine();
		
		//If there still are items in the input file
		if (line != null) {
			String[] lineSplitted = line.split(";");
			
			/* Check if the key is already in the hashtable. If it is the case we update the aggregate the item
			 * to the corresponding entry of the hashtable, else we add the item as a new element at the end of
			 * the hashtable if it is not full.
			 */
			if (outputTable.containsKey(lineSplitted[positionGroup])) {
				if(threadNumber == 0) {
					outputTable.put(lineSplitted[positionGroup], outputTable.get(lineSplitted[positionGroup]) + Integer.parseInt(lineSplitted[positionGroup + 1]));
				} else {
					outputTable.put(lineSplitted[positionGroup], outputTable.get(lineSplitted[positionGroup]) + 1);
				}
			}
			else {
				// Check if there is place in memory, if not we write the item in the overflow file
				if (outputTable.size() < MainForTest.MEMORY_SIZE) {
					if(threadNumber == 0) {
						outputTable.put(lineSplitted[positionGroup], Integer.parseInt(lineSplitted[positionGroup + 1]));
					} else {
						outputTable.put(lineSplitted[positionGroup], 1);
					}
				}
				else {
					overflowFile.writeLine(line);
				}
			}
			return true;		
		}
		
		// This corresponds to the case where the end of the input file is reached
		else {
			
			// We write the content of the hashtable in the output file and then make it empty 
		    // System.out.println("Writing in output...");
			this.writeHashTableInOutput();
			this.outputTable.clear();
			
			// If the overflow is empty then we finish else we change the input by the overflow
			// We close the overflow file to make it save its modifications
			this.overflowFile.closeFile();
			File overflow = new File(fileNameOverflow);
			
			if(overflow.length() == 0) {
				//Delete the overflow file
				overflow.delete();
				//We finish		
				return false;
			}
			else {
				// Delete the old input file (if it is not the initial input file)
				this.inputFile.closeFile();
				if(this.isTemporaryInput) {
					this.deleteTemporaryFile();
				}
				
				// Rename overflow file in temporary input file and save modifications
				overflow.renameTo(new File(this.fileNameTemporary));
				
				// Update the inputFile attribute and reset the overflow file
				this.isTemporaryInput = true;
				this.inputFile = new ReaderFile(this.fileNameTemporary);
				this.overflowFile = new WriterFile(this.fileNameOverflow);
	
				return true;
			}
			
		}
	}
	
	// Return the hashtable
	public Hashtable<String, Integer> getItemsOutput() {
		return this.outputTable;
	}
	
	// Return the hashtable keys
	public Enumeration<String> getKeys() {
		return this.outputTable.keys();
	}
	
	public boolean getIsTemporaryInput() {
		return this.isTemporaryInput;
	}
	
	public void deleteTemporaryFile() {
		this.inputFile.deleteFile();
	}
	
	// Write the hashtable in an output file
	public void writeHashTableInOutput() {
		Iterator<Entry<String, Integer>> itr = outputTable.entrySet().iterator();
		 
		Map.Entry<String, Integer> entry = null;
		while(itr.hasNext()) {   
		    entry = itr.next();	    
		    String line = entry.getKey() + " ;" + entry.getValue();
		    outputFile.writeLine(line);
		}
	}   

}