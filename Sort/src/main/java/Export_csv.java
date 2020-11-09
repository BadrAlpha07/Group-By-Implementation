/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Map;

/**
 *
 * @author asus
 */
public class Export_csv {
     public static String[][] export_csv(Map<Integer , Integer> res,
             String csv_file) throws FileNotFoundException {
     try (PrintWriter writer = new PrintWriter(new File(csv_file))) {
     StringBuilder builder = new StringBuilder();
     builder.append("Group Name");
     builder.append(";");
     builder.append("Count");
     builder.append("\r\n");
     for (Map.Entry<Integer, Integer> kvp : res.entrySet()) {
             builder.append(kvp.getKey());
             builder.append(";");
             builder.append(kvp.getValue());
             builder.append("\r\n");
}
     String content = builder.toString().trim();
     writer.write(builder.toString()); 
}
       return null;
}
}