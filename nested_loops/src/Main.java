import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Scanner;

import javax.swing.JFileChooser;

public class Main {
	public static void main(String args[]) {
		
		JFileChooser chooser = new JFileChooser();
		File workingDirectory = new File(System.getProperty("user.dir"));
		chooser.setCurrentDirectory(workingDirectory);
        int status = chooser.showOpenDialog(null);
        if (status == JFileChooser.APPROVE_OPTION) {
            File file = chooser.getSelectedFile();
            if (file == null) {
                return;
            }

            String fileName = chooser.getSelectedFile().getAbsolutePath();
            
            InputFile input = new InputFile(fileName);
    		OutputFile output = new OutputFile("./data/output.csv");
            
            String line = input.readLine();
    		//Here we print the first row to see our column
    	    System.out.println(line);
    		String[] split_line = line.split(";");
    		int max_position = split_line.length ;
    	    int position = -1;
     
    		
    	    //We want to get the position of the column that we want 
    	    while(position<0 || position >= max_position) {
    	    	Scanner myObj = new Scanner(System.in);  // Create a Scanner object
    		    System.out.println("Enter the position of the column we want to group by");
    		    String userName = myObj.nextLine();  // Read user input
    		    position = Integer.parseInt(userName);
    		    
    	    }
    	    ItemsOutput items = new ItemsOutput(input, output, position);
    	    
    	    boolean check = true;
    	    while (check) {
    	    	check = items.addItem();
    	    }
    	    
    	    System.out.println("Fin procedure");
    	    output.closeFile();
    	    input.closeFile();
    	    
    	    if(items.getIsTemporaryInput()) {
    	    	items.deleteTemporaryFile();
    	    }

        }		
	}
}
