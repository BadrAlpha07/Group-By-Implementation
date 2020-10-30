import java.util.ArrayList;

public class NestedLoopsMultiThread {
	
	private int positionGroup;
	private int nbThreads;
	
	//private OutputFile output = null;
	private String inputName = "";
	//private final Thread[] threads;
	
	public NestedLoopsMultiThread(String inputName, int positionGroup, int nbThreads) {
		this.positionGroup = positionGroup;
		this.nbThreads = nbThreads;
		//this.output = output;
		this.inputName = inputName;
	}
	
	public void apply() {
		
		this.splitInput();
		String beginInputName = this.inputName.substring(0, this.inputName.length() - 4);
		
		ArrayList<Thread> threads = new ArrayList<>();
		
        for(int i = 0; i < nbThreads; ++i) {
        	NestedLoopsSingleThread nestedLoops = new NestedLoopsSingleThread(beginInputName, i, positionGroup);
        	Thread thread = new Thread(nestedLoops);
            thread.start();
            threads.add(thread);
        }
        
        for (Thread t : threads) {
            try {
				t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
	}
	
	public void splitInput() {
		InputFile input = new InputFile(this.inputName);
		int recordsNumber = input.getRecordsNumber();
		input.readLine();
		String line = input.readLine();
		System.out.println(inputName);
		String nameBlockInput = this.inputName.substring(0, this.inputName.length() - 4);
		int count = 0;
		int numberRecordsBlock = recordsNumber / this.nbThreads;
		
		OutputFile[] blockInputs = new OutputFile[this.nbThreads];
		for(int i=0; i < this.nbThreads; i++) {
			blockInputs[i] = new OutputFile(nameBlockInput + Integer.toString(i) + ".csv");
		}
		
		while(line != null) {
			int blockNumber = count / numberRecordsBlock;
			if(blockNumber < this.nbThreads) {
				blockInputs[blockNumber].writeLine(line);
			} else {
				blockInputs[blockNumber - 1].writeLine(line);
			}
			count++;
			line = input.readLine();
		}
		
		for(int i=0; i < nbThreads; i++) {
			blockInputs[i].closeFile();
		}
		
		input.closeFile();
	}

}
