import java.io.File;
import java.util.Scanner;
import javax.swing.JFileChooser;

public class Main {
	
	public static final int MEMORY_SIZE = 1000;
	public static final String TMP_PATH = "./tmp/";
	public static final String FILE_TYPE = ".csv";
	
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

            String inputName = chooser.getSelectedFile().getAbsolutePath();
            File temp = new File(TMP_PATH);
    		temp.mkdirs();
            
            ReaderFile input = new ReaderFile(inputName);
            
            String line = input.readLine();
    		//Here we print the first row to see our column
    	    System.out.println(line);
    		String[] splittedLine = line.split(";");
    		int maxPosition = splittedLine.length ;
    	    input.closeFile();
     
    	    Scanner sc = new Scanner(System.in);        
    	    System.out.println("Enter the position of the column you want to group by:");
    	    int positionGroup = sc.nextInt();
    	    
    	    //Ask the user the positionGroup of the column he wants to group by
    	    while(positionGroup<0 || positionGroup >= maxPosition) {
    	    	System.out.println("please try again!");
    	        positionGroup = sc.nextInt();
    	    }
    	    
    	    System.out.println("Enter the number of threads you want:");
    	    int nbThreads = sc.nextInt();
    	    
    	    while(nbThreads < 1) {
    	    	System.out.println("please try again!");
    	        nbThreads = sc.nextInt();
    	    }
    	    sc.close();
    	    
    	    //Measure the execution time
    	    long t1 = System.nanoTime();
    	    
    	    NestedLoopsMultiThread nestedLoops = new NestedLoopsMultiThread(inputName, positionGroup, nbThreads);
    	    nestedLoops.apply();
    	    
            long t2 = System.nanoTime();
            long timing = (t2-t1)/(1000000*10);
            System.out.format("Processing time: %d ms\n", timing);
    	  
        }		
	}
}
