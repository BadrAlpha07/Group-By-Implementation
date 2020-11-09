/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author BERRADI Aymane
 */

import java.util.Scanner;
import java.util.stream.*;
import java.util.*;
import java.util.HashMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class BIGDATASQUAD {
        // One hot encoding function to transform strings to numbers.
        public static HashMap<Hashtable<String, Integer>, int []> one_hot_encoding(String tab [],String [] unique){
        Hashtable<String, Integer> my_dict = new Hashtable<String, Integer>();
        HashMap<Hashtable<String, Integer>, int []> hm=new HashMap<>();
        int[] index=new int[tab.length];
        for (int i=0;i<unique.length;i++)
        {
            for (int j=0;j<tab.length;j++)
            {
                if (tab[j] == null ? unique[i] == null : tab[j].equals(unique[i]))
                {   
                    index[j]=i;
                    my_dict.put(tab[j], i); 
                }
            }
        }
        hm.put(my_dict, index);
        return hm; 
    
}

    // Selection Sort on integer array.
        public static int []  Selection_Sort_array(int arr[])
        {
       int size, i, j,temp; 
       size=arr.length;
       for(i=0; i<size; i++)  
       {  
           for(j=i+1; j<size; j++)  
           {  
               if(arr[i] > arr[j])  
               {   
                   temp = arr[i];  
                   arr[i] = arr[j];  
                   arr[j] = temp;  
               }  
           }  
       } 
       return arr;
        }
        // Selection Sort on matrix.
        public static String [][]  Selection_Sort_matrix(String matrix [][]) 
       { 
       int size, i, j; 
       String[] temp=new String [matrix.length];
       size=matrix.length;
       for(i=1; i<size; i++)  
       {  
           for(j=i+1; j<size; j++)  
           {  
               if(Integer.parseInt(matrix[i][1]) > Integer.parseInt(matrix[j][1]))  
               {   
                   temp = matrix[i];  
                   matrix[i] = matrix[j];  
                   matrix[j] = temp;  
               }  
           }  
       } 
       return matrix;
       }
        // get the key of a given value of a hashtable.
        public static <K, V> K getKey(Map<K, V> map, V value) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (value.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }
       // Convert Integer [] to int [].
       public static int[] toPrimitive(Integer[] IntegerArray) {
		int[] result = new int[IntegerArray.length];
		for (int i = 0; i < IntegerArray.length; i++) {
			result[i] = IntegerArray[i].intValue();
		}
		return result;
	}
       
        // Aggregation of sorted values including (count, average, min, max, sum).
       public static float [] []  Aggregation(int [] tab,String [][] matrix) {
           int count=0;
           int pas=1;
           float sum=0;
           float avg=0;
           float result [][] = new float [tab.length][5];
           
           for (int k=0;k<tab.length;k++)
            { List<Integer> values = new ArrayList<>();
                for (int m = pas ; m < matrix.length ; m++) 
                {
                    if (Integer.parseInt(matrix[m][1]) == tab[k]) 
                        {   
                            sum = sum + Integer.parseInt(matrix[m][2]);
                            count = count + 1;
                            avg = sum / count;
                            values.add(Integer.parseInt(matrix[m][2]));
                        }
                    else{                        
                        break;
                    }
      
                }
            int[] aggregations=values.stream().mapToInt(i -> i).toArray();
            aggregations=Selection_Sort_array(aggregations);
            result[k][0]=(int) count;
            result[k][1]=avg;
            result[k][2]=aggregations[0];
            result[k][3]=aggregations[aggregations.length-1];
            result[k][4]=sum;
            pas=pas+count;
            count=0; 
            sum=0;
  
            }
        return result;
       }
    
}


    