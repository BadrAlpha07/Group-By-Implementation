package org.ulysse.project_maven;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

// This class models a file where we can write lines
public class WriterFile {
	
	private BufferedWriter writer = null;
	private String fileName = null;
	
	public WriterFile(String fileName) {
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(fileName))));
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.fileName = fileName;
	}
	
	// Return the BufferedReader 
	public BufferedWriter getReader() {
		return writer ;
	}
	
	// Return the name of the file 
	public String getFileName() {
		return fileName;
	}
	
	// Return one line from the file
	public boolean writeLine(String line) {
		try {
			writer.write(line);
			writer.newLine();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		 
	}
	
	// Close the file
	public void closeFile() {
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}