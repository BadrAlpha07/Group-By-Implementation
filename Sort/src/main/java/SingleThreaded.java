

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class SingleThreaded {
     public static String [][]  Insertion_Sort_matrix(String matrix [][], int col) 
    { 
        int n = matrix.length;
       
        for (int i = 2; i < n; ++i) { 
            int key = Integer.parseInt(matrix[i][col]); 
            int j = i - 1; 
  
            /* Move elements of arr[0..i-1], that are 
               greater than key, to one position ahead 
               of their current position */
            while (j >= 1 && Integer.parseInt(matrix[j][col]) > key) { 
                matrix[j + 1] = matrix[j]; 
                j = j - 1; 
            } 
            matrix[j+1][col] = String.valueOf(key); 
        } 
        return matrix;
    }
           public static String [][]  Selection_Sort_matrix(String matrix [][], int col) 
       { 
       int size, i, j; 
       String[] temp=new String [matrix.length];
       size=matrix.length;
       for(i=0; i<size; i++)  
       {  
           for(j=i+1; j<size; j++)  
           {  
               if(Integer.parseInt(matrix[i][col]) > Integer.parseInt(matrix[j][col]))
               {   
                   temp = matrix[i];  
                   matrix[i] = matrix[j];  
                   matrix[j] = temp;  
               }  
               
           }  
       } 
       
       return matrix;
       }
          public static String [][]  MergeSort(String [][] matrix, int n,int col ) {
        

        if (n < 2) {
        return matrix ;
    }
        
    int midpoint = (int) n / 2;
    String[][] LEFT = new String[midpoint][];
    String[][] RIGHT = new String[n - midpoint][];
    
 
    for (int i = 0; i < midpoint; i++) {
            LEFT[i] = matrix[i];    
    }
    for (int i = midpoint; i < n; i++) {
        RIGHT[i - midpoint] = matrix[i];
    }
    MergeSort(LEFT, midpoint,col);
    MergeSort(RIGHT, n - midpoint,col);
    return merge(matrix, LEFT, RIGHT, midpoint, n - midpoint,col);
    }
    public static String[][] merge(
  String[][] a, String[][] LEFT, String[][] RIGHT, int left, int right,int col) {
 
    int i = 0, j = 0, k = 0;
    while (i < left && j < right) {
        
        if (Integer.parseInt(LEFT[i][col]) <= Integer.parseInt(RIGHT[j][col])) {
            a[k++] = LEFT[i++];
        }
        else {
            a[k++] = RIGHT[j++];
        }
    }
    while (i < left) {
        a[k++] = LEFT[i++];
    }
    while (j < right) {
        a[k++] = RIGHT[j++];
    }
    return a;
}
    public static String [][] sort(String [][] matrix,int c) 
    { 
        int n = matrix.length; 
  
        // Build heap (rearrange array) 
        for (int i = n / 2 - 1; i >= 0; i--) 
            heapify(matrix, n, i,c); 
  
        // One by one extract an element from heap 
        for (int i=n-1; i>0; i--) 
        { 
            // Move current root to end 
            String [] temp = matrix[0]; 
            matrix[0] = matrix[i]; 
            matrix[i] = temp; 
  
            // call max heapify on the reduced heap 
            heapify(matrix, i, 0,c); 
        }
        return matrix;
    } 
    static void heapify(String [][] matrix, int n, int i,int col) 
    { 
        int largest = i; // Initialize largest as root 
        int l = 2*i + 1; // left = 2*i + 1 
        int r = 2*i + 2; // right = 2*i + 2 
  
        // If left child is larger than root 
        if (l < n && Integer.parseInt(matrix[l][col]) > Integer.parseInt(matrix[largest][col])) 
            largest = l; 
  
        // If right child is larger than largest so far 
        if (r < n && Integer.parseInt(matrix[r][col]) > Integer.parseInt(matrix[largest][col])) 
            largest = r; 
  
        // If largest is not root 
        if (largest != i) 
        { 
            String [] swap = matrix[i]; 
            matrix[i] = matrix[largest]; 
            matrix[largest] = swap; 
  
            // Recursively heapify the affected sub-tree 
            heapify(matrix, n, largest,col); 
        } 
    }
     
     public static Map<Integer , Integer>  Aggregation(String [][] matrix,int col) {
           Map<Integer , Integer> map = Collections.synchronizedMap(new LinkedHashMap<Integer , Integer>());
           int count=0;
           int pas=0;
           int k=0;
           int m=0;
           do 
            {
                
                for (m = k ; m < matrix.length ; m++) 
                {
                    if (Integer.parseInt(matrix[k][col])==Integer.parseInt(matrix[m][col]))
                           
                        {   
                            count++;
                           
                        }
                    
                    else { 
                        break;
                    }
                }
                map.put(Integer.parseInt(matrix[k][col]),count);

                pas=pas+count;
                k=pas;
                count=0;
                
            } while (k<matrix.length);
        return map;
       }
     
     public static Iterable<Map.Entry<Integer, Integer>> groupbySort (Iterator<String[]> x,int col) {
       // Read csv file into matrix.
       
        List<String[]> mutableList = new ArrayList<>();
        x.forEachRemaining(mutableList::add);
        String [][] matrix = new String [mutableList.size()][];
        for (int i=0;i<matrix.length;i++) {

            matrix[i]=mutableList.get(i);
        }
       
       matrix=MergeSort(matrix,matrix.length,col);// Merge sort algorithm
       //matrix=sort(matrix,col); // Heap sort algorithm
       //matrix=Selection_Sort_matrix(matrix,col); // Selection sort algorithm
       Map<Integer, Integer> arr2 = new Hashtable<Integer, Integer>();
       arr2=Aggregation(matrix,col);
       Set<Map.Entry<Integer, Integer>> iterableOutputTable = arr2.entrySet();
       return iterableOutputTable;
}
     public  Map<Integer , Integer> SingleThreadSort(String [][] matrix, int col) throws FileNotFoundException
     {   
         
         matrix=MergeSort(matrix,matrix.length,col);
         //matrix=sort(matrix,col); // Heap sort algorithm
         //matrix=Selection_Sort_matrix(matrix,col); // Selection sort algorithm
         Map<Integer , Integer> arr2 = Collections.synchronizedMap(new LinkedHashMap<Integer , Integer>());
         arr2=Aggregation(matrix,col);
         //Export_csv save = new Export_csv();
         //save.export_csv(arr2,"result.csv");
         return arr2;
     }
}
