/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author asus
 */
import java.io.BufferedReader; 
import java.io.FileReader;
import java.util.ArrayList; 
import java.util.List;


public class Read_CSV {
        public static String[][] read_csv(String path, int remove){
        List<String[]> rowList = new ArrayList<String[]>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            int s=0;
            while ((line = br.readLine()) != null) {
                
            String[] lineItems = line.split(";");
            
                if (s==0 && remove==0){
                    rowList.add(lineItems); 
                }
                if (s>=1) {
                rowList.add(lineItems);}
            s++;
            }
            br.close();
        }
        catch(Exception e){
            System.out.println("not found");
            // Handle any I/O problems
        }
        String[][] matrix = new String[rowList.size()][];
        for (int i = 0; i < rowList.size(); i++) {
            String[] row = rowList.get(i);
            matrix[i] = row;
        }
       // System.out.println(row);
       return matrix;
        }
}
