
import java.util.*;
import java.util.Map.Entry;
import java.io.File;
	
public class NestedLoopsSingleThread implements Runnable {

	//Hashtable simulating the memory, with a maximum size of MEMORY_SIZE
	private Hashtable<String, Integer> outputTable = new Hashtable<String, Integer>();
	
	private String fileNameTemporary = "";
	private String fileNameOverflow = "";
	private int threadNumber = 0;
	
	//Overflow file stores items that do not fit in memory
	private WriterFile overflowFile = null;
	private WriterFile outputFile = null;
	private ReaderFile inputFile = null;
	
	private int positionGroup; //position of the column to group by
	private boolean isTemporaryInput = false;
	
	
	//Constructor 
	public NestedLoopsSingleThread(String beginInputName, int threadNumber, int position) {
		this.positionGroup = position ;
		this.threadNumber = threadNumber;
		this.fileNameOverflow = Main.TMP_PATH + "overflow" + Integer.toString(threadNumber) + Main.FILE_TYPE;
		this.fileNameTemporary = Main.TMP_PATH + "temporary" + Integer.toString(threadNumber) + Main.FILE_TYPE;
		
		if(threadNumber == 0) {
			this.outputFile = new WriterFile(Main.TMP_PATH + "output" + Main.FILE_TYPE);
			this.inputFile = new ReaderFile(beginInputName);
		} else {
			this.outputFile = new WriterFile(Main.TMP_PATH + "output" + Integer.toString(threadNumber) + Main.FILE_TYPE);
			this.inputFile = new ReaderFile(beginInputName + Integer.toString(threadNumber) + Main.FILE_TYPE);
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
	
	
	//Add item in the Hashtable
	//return true if we are already reading the input file or the overflow is not empty
	//return false if we are at the end of the input file and the overflow is empty
	public boolean processInputItem() {
		//Check if the key is already in Hashtable.
		//If it is the case we update the aggregation value,
		//else we add the item as a new element in the Hashtable
		String line = inputFile.readLine();
		
		//If there still are items in the input file
		if (line != null) {
			//Get line by column and choose the column we want
			String[] lineSplitted = line.split(";");
			if (outputTable.containsKey(lineSplitted[positionGroup])) {
				if(threadNumber == 0) {
					outputTable.put(lineSplitted[positionGroup], outputTable.get(lineSplitted[positionGroup]) + Integer.parseInt(lineSplitted[positionGroup + 1]));
				} else {
					outputTable.put(lineSplitted[positionGroup], outputTable.get(lineSplitted[positionGroup]) + 1);
				}
			}
			else {
				//We check if there is place in memory, if not we write the information in the overflow file
				if (outputTable.size() < Main.MEMORY_SIZE) {
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
		//If the end of the input file is reached
		else {
			//We write the content of the Hashtable in the output file and clean it 
		    System.out.println("Writing in output...");
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
				//Delete the old input file
				this.inputFile.closeFile();
				if(this.isTemporaryInput) {
					this.deleteTemporaryFile();
				}
				
				//Rename overflow file in input file and save modifications
				overflow.renameTo(new File(this.fileNameTemporary));
				
				//Update the inputFile attribute and reset the overflow file
				this.isTemporaryInput = true;
				this.inputFile = new ReaderFile(this.fileNameTemporary);
				this.overflowFile = new WriterFile(this.fileNameOverflow);
	
				return true;
			}
			
		}
	}
	
	//Return the Hashtable
	public Hashtable<String, Integer> getItemsOutput() {
		return this.outputTable;
	}
	
	public Enumeration<String> getKeys() {
		return this.outputTable.keys();
	}
	
	public boolean getIsTemporaryInput() {
		return this.isTemporaryInput;
	}
	
	public void deleteTemporaryFile() {
		this.inputFile.deleteFile();
	}
	
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