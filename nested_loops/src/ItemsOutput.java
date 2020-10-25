
import java.util.*;
import java.util.Map.Entry;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class ItemsOutput {
	
	public final static int MAX_ITEM = 10;
	private Hashtable<String, Integer> outputTable= new Hashtable<String, Integer>();
	private String fileNameOverflow = "./data/overflow.csv";
	private String fileNameTemporary = "./data/temporary.csv";
	
	private OutputFile overflowFile = new OutputFile(fileNameOverflow);
	private  OutputFile outputFile = null;
	private InputFile inputFile = null ; 
	private int positionGroup ;
	private boolean isTemporaryInput = false;
	
	//Constructor 
	public ItemsOutput (InputFile input,OutputFile output, int position) {
		this.outputFile = output ;
		this.inputFile = input ; 
		this.positionGroup = position ;
	}
	
	
	//Add item in the hashtable
	//return true if we are already reading the input file or the overflow is not empty
	//return false if we are at the end of the input file and the overflow is empty
	public boolean addItem() {
		//Check if the key is already in hashtable, is it's the case we update his value, else we put the new value to the hashtable
		String line = inputFile.readLine();
		String fileNameInput = inputFile.getFileName();
		//We check that we have items in input file
		if (line != null) {
			//get line by column and choose the column we want
			String[]line_split = line.split(";");
			if (outputTable.containsKey(line_split[positionGroup])) {
				outputTable.put(line_split[positionGroup], outputTable.get(line_split[positionGroup]) + 1);
			}
			
			else {
				//We check if there is place in memory, if it's not the case we write the information in overflow
				if (outputTable.size() < MAX_ITEM) {
					outputTable.put(line_split[positionGroup],1);
				}
				else {
					overflowFile.writeLine(line);
				}
			}
			return true;		
		}
		else {
			//We write the hashtable in output and clean it 
		    System.out.println("Writing in output");
			this.writeHashTableInOutput();
			this.outputTable.clear();
			
			// If the overflow is empty then we finish else we change the input by the overflow
			// We close the overflow file to make it save its modifications
			this.overflowFile.closeFile();
			File over = new File(fileNameOverflow);
			
			if(over.length() == 0) {
				// Delete the overflow file
				over.delete();
				//We finish		
				return false;
			}
			else {
				//delete the old input file
				this.inputFile.closeFile();
				if(this.isTemporaryInput) {
				this.deleteTemporaryFile();
				}
				
				//rename overflow file in input file and save modifications
				over.renameTo(new File(this.fileNameTemporary));
				this.overflowFile.closeFile();

				// update the inputFile attribute and reset the overflow file
				this.isTemporaryInput = true;
				this.inputFile = new InputFile(this.fileNameTemporary);
				this.overflowFile = new OutputFile(this.fileNameOverflow);

				return true;
			}
			
		}
	}
	
	//Return the hashTable
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
		    System.out.println("Writing in output");
		    String line = entry.getKey()+ " ;" +entry.getValue();
		    outputFile.writeLine(line);

		}
	}
	
}

