
public class NestedLoopsMultiThread {
	
	private int positionGroup;
	private int nbThreads;
	
	private OutputFile output = null;
	private InputFile input = null;
	//private final Thread[] threads;
	
	public NestedLoopsMultiThread(InputFile input, OutputFile output, int positionGroup, int nbThreads) {
		this.positionGroup = positionGroup;
		this.nbThreads = nbThreads;
		this.output = output;
		this.input = input; 
		
		//this.threads = new Thread[nbThreads];
	}
	
	public void apply() { 
        for(int i = 0; i < nbThreads; ++i) {
        	OutputFile out = new OutputFile("output" + Integer.toString(i));
        	NestedLoopsSingleThread thread = new NestedLoopsSingleThread(input, out, positionGroup);
        	thread.start();
        }        
	}

}
