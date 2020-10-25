import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Scanner;

public class Main {
	public static void main(String args[]) {
		
		InputFile input = new InputFile("/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/input.txt");
		OutputFile output = new OutputFile("/Users/nadirabdou/eclipse-workspace/Nested_loop/group-by/nested_loops/src/output.txt");
		
		String line = input.readLine();
		//Here we print the firt row to see our column
	    System.out.println(line);
		String[] split_line = line.split(",");
		int max_position = split_line.length ;
	    int position = -1;
 
		
	    //We want to get the position of the column that we want 
	    while(position<0 || position >= max_position) {
	    	Scanner myObj = new Scanner(System.in);  // Create a Scanner object
		    System.out.println("Enter the position of the column we want to group by");
		    String userName = myObj.nextLine();  // Read user input
		    position = Integer.parseInt(userName);
		    
	    }
	    ItemsOutput items = new ItemsOutput(input,output,position);
	    
	    boolean check = true;
	    while (check) {
	    	check = items.addItem();
	    }
	    
	    System.out.println("Fin procédure");
	    output.closeFile();
		input.closeFile();
		File f = new File("Moooh");
	}
}
