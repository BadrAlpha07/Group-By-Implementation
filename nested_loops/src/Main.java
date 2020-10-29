import java.io.File;
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
    		String[] splittedLine = line.split(";");
    		int maxPosition = splittedLine.length ;
    	    int position = -1;
     
    		
    	    //Ask the user the position of the column he wants to group by
    	    while(position<0 || position >= maxPosition) {
    	    	Scanner scanner = new Scanner(System.in);  // Create a Scanner object
    		    System.out.println("Enter the position of the column you want to group by:");
    		    String positionString = scanner.nextLine();  // Read user input
    		    position = Integer.parseInt(positionString);
    		    scanner.close();
    	    }
    	    ItemsOutput items = new ItemsOutput(input, output, position);
    	    
    	    boolean check = true;
    	    while (check) {
    	    	check = items.addItem();
    	    }
    	    
    	    System.out.println("Processing finished");
    	    output.closeFile();
    	    input.closeFile();
    	    
    	    if(items.getIsTemporaryInput()) {
    	    	items.deleteTemporaryFile();
    	    }

        }		
	}
}
