import java.io.File;
import java.util.Scanner;
import javax.swing.JFileChooser;

public class Main {
	public static void main(String args[]) {
		
		int nbProcessors = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of processors available: " + Integer.toString(nbProcessors));
        
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
    	    int positionGroup = -1;
     
    		
    	    //Ask the user the positionGroup of the column he wants to group by
    	    while(positionGroup<0 || positionGroup >= maxPosition) {
    	    	Scanner scanner = new Scanner(System.in);  // Create a Scanner object
    		    System.out.println("Enter the positionGroup of the column you want to group by:");
    		    String positionString = scanner.nextLine();  // Read user input
    		    positionGroup = Integer.parseInt(positionString);
    		    scanner.close();
    	    }
    	    
    	    //Measure the execution time
    	    long t1 = System.nanoTime();
    	    
    	    NestedLoopsSingleThread nestedLoops = new NestedLoopsSingleThread(input, output, positionGroup);
    	    nestedLoops.start();
    	    
//    	    int nbThreads = 1;
//    	    NestedLoopsMultiThread nestedLoops = new NestedLoopsMultiThread(input, output, positionGroup, nbThreads);
//    	    nestedLoops.start();
    	    
            long t2 = System.nanoTime();
            long timing = (t2-t1)/(1000000*10);
            System.out.format("Processing time: %d ms\n", timing);
    	    
    	    output.closeFile();
    	    input.closeFile();
        }		
	}
}
