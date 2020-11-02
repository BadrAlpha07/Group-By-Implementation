package groupBy;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;


public class Main {

    public static void main(String[] args) {

        String csvFile = "src/test.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        Map<String, ArrayList<Integer>> map = new HashMap<String, ArrayList<Integer>>();
        
        try {

            br = new BufferedReader(new FileReader(csvFile));
            int rowNum = 0;
            
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] row = line.split(cvsSplitBy);
                if (!map.containsKey(row[2]) && rowNum != 0) {

                	ArrayList arr = new ArrayList();
                	map.put(row[2], arr);
                	
                }
                if (rowNum != 0) {
                	map.get(row[2]).add(Integer.parseInt(row[3]));
                }
                
                //System.out.println("Country [code= " + row[2] + " , name=" + row[3] + "]");
                
                rowNum ++;
                //System.out.println(rowNum);
            }
            System.out.println(map);
            
            
            // aggregate
            Map<String, Integer> result = new HashMap<String, Integer>();
            
            Iterator<String> iterator=map.keySet().iterator();
            while(iterator.hasNext())
            {
                String k=iterator.next();
                int sum = 0;
                for (int i = 0; i < map.get(k).size(); i++) {
                	sum += map.get(k).get(i);
                }
                result.put(k, sum);
                System.out.println(k+" "+sum);
            }
            

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
	
}
