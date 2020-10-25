
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
	private BufferedWriter overflow = null;
	private  BufferedWriter output = null;
	private InputFile input = null ; 
	private int positionGroup ;
	
	//Constructeur 
	public ItemsOutput (InputFile input,int position) {
		try {
			overflow = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/overflow.txt"))));
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/output.txt"))));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.input = input ; 
		positionGroup = position ;
	}
	
	
	//Add item in the hastable
	//return 1 if we read the input file and we add an item in hashtable or overflow
	//return -1 if we don't have anything un input file  and the overflow is empty
	//return 0 when we have to write overflow in input 
	public int addItem() {
		//Check if the key is already in hshtable, is it's the case we update his value, else we put the new value to th hashtabe
		String line = input.readLine();
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
					try {
					    System.out.println("Writing in overflow");
						this.overflow.write(line_split[positionGroup]+' '+1);
						this.overflow.newLine();
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
		    System.out.println("Writing in output");
			writeHashTableInOutput();
			itemsOutput.clear();
			//If the overflow is empty then we finish else we change the input by the overflow
			File over = new File("/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/overflow.txt");
			//NB: Je dois essayer récupérer le nom à partir du buffer input 
			File inp = new File("/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/input.txt");
			if(over.length() == 0) {
				try {
					output.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					overflow.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return -1;
			}
			else {
				//delet the old input file

				inp.delete();
				//rename overflow file in input file 
				over.renameTo(new File("/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/input.txt"));
				//create a new InputFile 
				//NB: Je pense que ça marche pas ça, car si on crée un truc il va pas étre mis à jour une fois qu'on sort de la fonction 
				try {
					overflow.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//Faut peut être faire ça dans le main 
				input = new InputFile("/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/input.txt");
				//create a new overflow
				try {
					overflow = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/overflow.txt"))));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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
			    System.out.println("Writing in output");
				this.output.write(entry.getKey()+','+entry.getValue());
				this.output.newLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}

