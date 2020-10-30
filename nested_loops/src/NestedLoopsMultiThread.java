import java.io.File;
import java.util.ArrayList;

public class NestedLoopsMultiThread {
	
	private int positionGroup;
	private int nbThreads;
	
	private WriterFile tempOutput = null;
	private String inputName = "";
	
	public NestedLoopsMultiThread(String inputName, int positionGroup, int nbThreads) {
		this.positionGroup = positionGroup;
		this.nbThreads = nbThreads;
		this.tempOutput = new WriterFile(Main.TMP_PATH + "temporaryOutput" + Main.FILE_TYPE);
		this.inputName = inputName;
	}
	
	public void apply() {
		
		this.splitInput();
		String beginInputName = this.inputName.substring(0, this.inputName.length() - 4);
		
		ArrayList<Thread> threads = new ArrayList<>();
		
        for(int i = 1; i <= nbThreads; i++) {
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
        
        this.concatenateOutputs();
        NestedLoopsSingleThread nestedLoops = new NestedLoopsSingleThread(tempOutput.getFileName(), 0, 0);
        Thread thread = new Thread(nestedLoops);
        thread.start();
        
        try {
			thread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        for(int i = 1; i <= nbThreads; ++i) {
        	File inp = new File(beginInputName + Integer.toString(i) + Main.FILE_TYPE);
        	inp.delete();
        }
        
        File tmpOut = new File(this.tempOutput.getFileName());
        tmpOut.delete();
	}
	
	public void splitInput() {
		ReaderFile input = new ReaderFile(this.inputName);
		int recordsNumber = input.getRecordsNumber();
		input.readLine();
		String line = input.readLine();
		String nameBlockInput = this.inputName.substring(0, this.inputName.length() - 4);
		int count = 0;
		int numberRecordsBlock = recordsNumber / this.nbThreads;
		
		WriterFile[] blockInputs = new WriterFile[this.nbThreads];
		for(int i=1; i <= this.nbThreads; i++) {
			blockInputs[i-1] = new WriterFile(nameBlockInput + Integer.toString(i) + Main.FILE_TYPE);
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
		
		for(int i=1; i <= nbThreads; i++) {
			blockInputs[i-1].closeFile();
		}
		
		input.closeFile();
	}
	
	public void concatenateOutputs() {
		for(int i=1; i <= this.nbThreads; i++) {
			ReaderFile input = new ReaderFile(Main.TMP_PATH + "output" + Integer.toString(i) + Main.FILE_TYPE);
			String line = input.readLine();
			
			while(line != null) {
				tempOutput.writeLine(line);
				line = input.readLine();
			}
			input.deleteFile();
		}
		tempOutput.closeFile();
	}

}
