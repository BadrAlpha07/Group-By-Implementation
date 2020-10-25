
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
 
public class InputFile {
	private BufferedReader reader = null;
	
	//Constructeur 
	public InputFile(String file_name) {
		try {
			reader = new BufferedReader(new FileReader(file_name));			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//Return the BufferReader 
	public BufferedReader getReader () {
		return reader ;
	}
	
	//Return one line from the file
	public String readLine() {
		BufferedReader reader_ = getReader();
		try {	
			String line = reader_.readLine();
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
		BufferedReader reader_ = getReader();
		try {
			reader_.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}