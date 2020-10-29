
import java.util.*;
import java.util.Map.Entry;
import java.io.File;
	
public class NestedLoopsSingleThread {

	//Hashtable simulating the memory, with a maximum size of MAX_ITEM
	public final static int MAX_ITEM = 100;
	private Hashtable<String, Integer> outputTable = new Hashtable<String, Integer>();
	
	private String fileNameOverflow = "./data/overflow.csv";
	private String fileNameTemporary = "./data/temporary.csv";
	
	//Overflow file stores items that do not fit in memory
	private OutputFile overflowFile = new OutputFile(fileNameOverflow);
	private OutputFile outputFile = null;
	private InputFile inputFile = null;
	
	private int positionGroup; //position of the column to group by
	private boolean isTemporaryInput = false;
	
	
	//Constructor 
	public NestedLoopsSingleThread(InputFile input, OutputFile output, int position) {
		this.outputFile = output ;
		this.inputFile = input ; 
		this.positionGroup = position ;
	}
	
	public void start() {
	    
	    boolean check = true;
	    while (check) {
	    	check = this.addItem();
	    }
	    
	    if(this.getIsTemporaryInput()) {
	    	this.deleteTemporaryFile();
	    }
	}
	
	
	//Add item in the Hashtable
	//return true if we are already reading the input file or the overflow is not empty
	//return false if we are at the end of the input file and the overflow is empty
	public boolean addItem() {
		//Check if the key is already in Hashtable.
		//If it is the case we update the aggregation value,
		//else we add the item as a new element in the Hashtable
		String line = inputFile.readLine();
		
		//If there still are items in the input file
		if (line != null) {
			//Get line by column and choose the column we want
			String[] lineSplitted = line.split(";");
			if (outputTable.containsKey(lineSplitted[positionGroup])) {
				outputTable.put(lineSplitted[positionGroup], outputTable.get(lineSplitted[positionGroup]) + 1);
			}
			else {
				//We check if there is place in memory, if not we write the information in the overflow file
				if (outputTable.size() < MAX_ITEM) {
					outputTable.put(lineSplitted[positionGroup], 1);
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
				this.inputFile = new InputFile(this.fileNameTemporary);
				this.overflowFile = new OutputFile(this.fileNameOverflow);
	
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
		this.inputFile.closeFile();
		File inp = new File(this.fileNameTemporary);
		inp.delete();
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