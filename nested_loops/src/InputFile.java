
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
 
public class InputFile {
	private BufferedReader reader = null;
	private String fileName = null;
	//Constructeur 
	public InputFile(String fileName) {
		try {
			reader = new BufferedReader(new FileReader(fileName));			
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.fileName = fileName;
	}
	
	//Return the BufferReader 
	public BufferedReader getReader () {
		return reader ;
	}
	
	//return the name of the file 
	public String getFileName() {
		return fileName;
	}
	
	//Return one line from the file
	public String readLine() {
		try {	
			String line = reader.readLine();
			if(line != null) {
				return line; 
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return null; 
	}
	
	//Fermer le fichier 
	public void closeFile() {
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}