
import java.util.*;
import java.util.Map.Entry;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class ItemsOutput {
	
	public final static int MAX_ITEM = 5;
	private Hashtable<String, Integer> itemsOutput= new Hashtable<String, Integer>();
	private String fileNameOutput = "/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/overflow.txt";
	private OutputFile overflow = new OutputFile(fileNameOutput);
	private  OutputFile output = null;
	private InputFile input = null ; 
	private int positionGroup ;
	
	//Constructeur 
	public ItemsOutput (InputFile input,OutputFile output, int position) {
		this.output = output ;
		this.input = input ; 
		positionGroup = position ;
	}
	
	
	//Add item in the hastable
	//return true if we are already reading the input file or the overflow is not empty
	//return false if we are at the end of the input file and the overflow is empty
	public boolean addItem() {
		//Check if the key is already in hshtable, is it's the case we update his value, else we put the new value to th hashtabe
		String line = input.readLine();
		String fileNameInput = input.getFileName();
		//We check that we have items in input file
		if (line != null) {
			//get line by column and choose the column we want
			String[]line_split = line.split(",");
			if (itemsOutput.containsKey(line_split[positionGroup])) {
				itemsOutput.put(line_split[positionGroup],itemsOutput.get(line_split[positionGroup])+1);
			}
			
			else {
				//We check if there is place in memory, if it's not the case we write the information in overflow
				if (itemsOutput.size()<MAX_ITEM) {
					itemsOutput.put(line_split[positionGroup],1);
				}
				else {
					overflow.writeLine(line);			
				}
			}
			return true;		
		}
		else {
			//We write the hashtable in output and clean it 
		    System.out.println("Writing in output");
			writeHashTableInOutput();
			itemsOutput.clear();
			//If the overflow is empty then we finish else we change the input by the overflow
			overflow.closeFile();
			File over = new File(fileNameOutput);
			//NB: Je dois essayer récupérer le nom à partir du buffer input 
			File inp = new File(fileNameInput);
			if(over.length() == 0) {
				//We finish 		
				return false;
			}
			else {
				//delet the old input file

				inp.delete();
				//rename overflow file in input file 

				over.renameTo(new File(fileNameInput));
				//create a new InputFile 
				//NB: Je pense que ça marche pas ça, car si on crée un truc il va pas étre mis à jour une fois qu'on sort de la fonction 
				overflow.closeFile();

				//Faut peut être faire ça dans le main 
				input = new InputFile(fileNameInput);
				
				//create a new overflow
				overflow = new OutputFile(fileNameOutput);

				return true;
			}
			
		}
	}
	
	//Return the hashTable
	public Hashtable<String, Integer> getItemsOutput() {
		return itemsOutput;
	}
	
	public Enumeration<String> getKeys() {
		return itemsOutput.keys();
	}
	
	public void writeHashTableInOutput() {
		Iterator<Entry<String, Integer>> itr = itemsOutput.entrySet().iterator();
		 
		Map.Entry<String, Integer> entry = null;
		while(itr.hasNext()) {   
		    entry = itr.next();	    
		    System.out.println("Writing in output");
		    String line = entry.getKey()+','+entry.getValue();
		    output.writeLine(line);

		}
	}
	
}

