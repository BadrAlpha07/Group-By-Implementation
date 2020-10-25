
import java.util.*;
import java.util.Map.Entry;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class ItemsOutput {
	
	public final static int MAX_ITEM = 100;
	private Hashtable<String, Integer> itemsOutput= new Hashtable<String, Integer>();
	private BufferedWriter overflow = null;
	private  BufferedWriter output = null;
	//private InputFile input = null ; 
	
	
	//Constructeur 
	public ItemsOutput (String inputFile) {
		try {
			overflow = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("overflow.txt"))));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("ouputt.txt"))));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	//Add item in the hastable
	//return 1 if we read the input file and we add an item in hashtable or overflow
	//return -1 if we don't have anything un input file  and the overflow is empty
	//return 0 when we have to write overflow in input 
	public int addItem(int position,InputFile input) {
		//Check if the key is already in hshtable, is it's the case we update his value, else we put the new value to th hashtabe
		String line = input.readLine();
		//We check that we have items in input file
		if (line != null) {
			//get line by column and choose the column we want
			String[]line_split = line.split(",");
			if (itemsOutput.containsKey(line_split[position])) {
				itemsOutput.put(line_split[position],itemsOutput.get(line_split[position])+1);
			}
			
			else {
				//We check if there is place in memory, if it's not the case we write the information in overflow
				if (itemsOutput.size()<MAX_ITEM) {
					itemsOutput.put(line_split[position],1);
				}
				else {
					try {
						overflow.write(line_split[position]+' '+1);
						overflow.newLine();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			return 1;		
		}
		else {
			//We write the hashtable in output and clean it 
			writeHashTableInOutput();
			itemsOutput.clear();
			//If the overflow is empty then we finish else we change the input by the overflow
			File over = new File("overflow.txt");
			//NB: Je dois essayer récupérer le nom à partir du buffer input 
			File inp = new File("input.txt");
			if(over.length() == 0) {
				return -1;
			}
			else {
				//delet the old input file
				inp.delete();
				//rename overflow file in input file 
				over.renameTo(new File("input.txt"));
				//create a new InputFile 
				//NB: Je pense que ça marche pas ça, car si on crée un truc il va pas étre mis à jour une fois qu'on sort de la fonction 
				//Faut peut être faire ça dans le main 
				input = new InputFile("input.txt");
				
				return 0;
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
		while(itr.hasNext()){
		    
		    entry = itr.next();
		    
		    try {
				output.write(entry.getKey()+' '+entry.getValue());
				output.newLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}

