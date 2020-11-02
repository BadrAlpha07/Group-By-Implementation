package org.ulysse.project_maven;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

// This class models a file where we can read each line and get the number of lines
public class ReaderFile {
	private BufferedReader reader = null;
	private String fileName = null;
	
	public ReaderFile(String fileName) {
		try {				
			reader = new BufferedReader(new FileReader(fileName));	
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.fileName = fileName;
	}
	
	// Return the BufferedReader 
	public BufferedReader getReader () {
		return reader ;
	}
	
	// Return the name of the file 
	public String getFileName() {
		return fileName;
	}
	
	// Get the number of lines contained in our file
	public int getRecordsNumber() {
		
		int lines = 0;
		
		try {				
			BufferedReader countReader = new BufferedReader(new FileReader(this.fileName));
			while (countReader.readLine() != null) lines++;
			countReader.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return lines;
	}
	
	// Return one line from the file
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
	
	// Close the file
	public void closeFile() {
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Close and delete the file
	public void deleteFile() {
		this.closeFile();
		File file = new File(this.fileName);
		file.delete();
	}
	
}