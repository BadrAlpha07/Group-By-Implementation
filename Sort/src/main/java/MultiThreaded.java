/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 *
 * @author asus
 */
public class MultiThreaded {
    int col=0;
    SingleThreaded grps=new SingleThreaded();
    public  class Groupby_Thread implements Callable<String[][]> {
        // Create  threads to group by sorting the data
        private String[][] matrix;

        public Groupby_Thread(String[][] matrix) {
            this.matrix = matrix;
        }
        
        @Override
        public String[][] call() throws Exception {
            matrix = MergeSort(matrix,matrix.length,0);
            String[][] out;
            out = Aggregation(matrix,0);
            return out;
        }
    }
     public  String [][]  Insertion_Sort_matrix(String matrix [][], int p) 
    { 
        int n = matrix.length;
        String [] key=new String[n];
        for (int i = 1; i < n; ++i) { 
            key = matrix[i]; 
            int j = i - 1; 
  
            /* Move elements of arr[0..i-1], that are 
               greater than key, to one position ahead 
               of their current position */
            if(p == 0){
                while (j >= 0 && Integer.parseInt(matrix[j][col]) > Integer.parseInt(key[col])) {
                    matrix[j + 1] = matrix[j];
                    j = j - 1;
                }}
            else if(p ==1){ // Case of Merging
                while (j >= 0 && Integer.parseInt(matrix[j][0]) > Integer.parseInt(key[0]) ) { 
                matrix[j + 1] = matrix[j]; 
                j = j - 1; 
            }}
            matrix[j+1] = key; 
        } 
        return matrix;
    }
     public  String [][]  Selection_Sort_matrix(String matrix [][], int p) 
       { 
       int size, i, j; 
       String[] temp=new String [matrix.length];
       size=matrix.length;

       for(i=0; i<size; i++)  
       {  
           for(j=i+1; j<size; j++)  
           {  
               if (p == 0 && Integer.parseInt(matrix[i][col]) > Integer.parseInt(matrix[j][col])) {
                   temp = matrix[i];  
                   matrix[i] = matrix[j];  
                   matrix[j] = temp; 
               } 
               else if(p == 1 && Integer.parseInt(matrix[i][0]) > Integer.parseInt(matrix[j][0])) {                   
                   temp = matrix[i];
                   matrix[i] = matrix[j];
                   matrix[j] = temp;
                   
               }
           }  
       } 
       return matrix;
       }

    public String [][]  MergeSort(String [][] matrix, int n,int p ) {
        

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
    MergeSort(LEFT, midpoint,p);
    MergeSort(RIGHT, n - midpoint,p);
    return merge(matrix, LEFT, RIGHT, midpoint, n - midpoint,p);
    }

    
    public  String[][] merge(
  String[][] a, String[][] LEFT, String[][] RIGHT, int left, int right,int p) {
        
    int i = 0, j = 0, k = 0,c=0;
    if(p==0){
    c= col;
    }
    while (i < left && j < right) {
        
        if (Integer.parseInt(LEFT[i][c]) <= Integer.parseInt(RIGHT[j][c])) {
            a[k] = LEFT[i];
            k++;
            i++;
        }
        else {
            a[k] = RIGHT[j];
            k++;
            j++;
        }
    }
    while (i < left) {
        a[k] = LEFT[i];
        k++;
        i++;
    }
    while (j < right) {
        a[k] = RIGHT[j];
        k++;
        j++;
    }
    return a;
}
    public  String [][] sort(String [][] matrix, int p) 
    { 
        int n = matrix.length; 
        int c=0;
         if(p == 0){ //case of Grouping 
                c=col;
                }
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
    void heapify(String [][] matrix, int n, int i,int col) 
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
       
        // Aggregation of sorted values including (count, average, min, max, sum).
       public  String[][]  Aggregation(String [][] matrix, int p) {
           Map<String , String> map = Collections.synchronizedMap(new LinkedHashMap<String , String>()); //We need it instead of matrix because of its variable size
           //String[][] map;
           int count=0;
           int pas=0;
           int i=0;
           int k=0;
           int m=0;
           int sum = 0;
           
           do 
            {                 
                for (m = k; m < matrix.length; m++) {
                    
                    if (p == 0 && Integer.parseInt(matrix[k][col]) == Integer.parseInt(matrix[m][col])) {
                        count++;
                    } else if (p == 1 && Integer.parseInt(matrix[k][0]) == Integer.parseInt(matrix[m][0])) {
                        sum = sum + Integer.parseInt(matrix[m][1]);
                        count++;
                    } else {
                        break;
                    }
                }
                
                if( p == 0) 
                {   
                    map.put(matrix[k][col],Integer.toString(count));
                   // System.out.println("Count("+Integer.parseInt(matrix[k][col])+","+count+")");
                }
                else if(p == 1)
                {
                    map.put(matrix[k][0],Integer.toString(sum));
                    //System.out.println("Sum("+Integer.parseInt(matrix[k][0])+","+sum+")");
                }
                
                pas=pas+count;
                k=pas;
                count=0;
                sum=0;
                
            } while (k<matrix.length);
           String[][] output = new String[map.size()][2];
           int t=0;
           for(Map.Entry<String,String> entry : map.entrySet()){ //We convert the map to matrix so it can be used in other processes 
                    output[t++] = new String[] { entry.getKey(), entry.getValue() };
            }
           return output;
    }
       public  String [][] Merge_parts(String [][] part1, String [][] part2) {
        // We merge the output of our threads after aggregation
        String[][] merged = new String[part1.length + part2.length][2];
        for (int i = 0; i < part1.length; i++) {
            merged[i][0] = part1[i][0];
            merged[i][1] = part1[i][1];
        }
        for (int i = 0; i < part2.length; i++) {
            merged[part1.length + i][0] = part2[i][0];
            merged[part1.length + i][1] = part2[i][1];
        }
        merged = MergeSort(merged,merged.length,1);
        String[][] out;
        out = Aggregation(merged, 1);
        return out;
    }
    public  String [][] GroupingMulti(String [][] matrix, int nb_threads,int cols) throws InterruptedException, ExecutionException, FileNotFoundException{
        //Main class for Multidread Group By using Sorting algorithms, which excutes the different steps
        col=cols;
        ExecutorService executor = Executors.newFixedThreadPool(nb_threads);
        List<Future<String[][]>> Futures = new ArrayList<>();
        int pas_part = (int) matrix.length / nb_threads;
        int t = 0;
        for(int i =0; i <matrix.length; i += pas_part) //We assign each thread an partition of the data
        {   
            t++; //check in which thread we are so we can avoid small subset at the end
            if( t < nb_threads){
                Groupby_Thread task = new Groupby_Thread(Arrays.copyOfRange(matrix, i, i+pas_part)); // allocate thread for each partition of the data
                //System.out.println(i);            
                Futures.add(executor.submit(task));}
            else{
                Groupby_Thread task = new Groupby_Thread(Arrays.copyOfRange(matrix, i, matrix.length)); // allocate thread for each partition of the data
                //System.out.println(i);            
                Futures.add(executor.submit(task));
                break;
            }
            
        }
        
        List<String[][]> outs = new ArrayList<>();
        for(int i = 0; i < Futures.size(); i++){ //We get the output of each task run by the threads
            outs.add(Futures.get(i).get());
        }
        
        String[][] sol = outs.get(0);
        for(int i  = 1; i < outs.size(); i++){
                String[][] out = outs.get(i);
            sol = Merge_parts(sol, out); // Merge the outputs of the threads
            
        }
       
        Map<Integer , Integer> arr2 = Collections.synchronizedMap(new LinkedHashMap<Integer , Integer>());
        for (int i=0;i<sol.length;i++) {
            arr2.put(Integer.parseInt(sol[i][0]),Integer.parseInt(sol[i][1]));
        }
        Export_csv save = new Export_csv();
        save.export_csv(arr2,"result.csv");
       
        //executor.shutdown();
        return sol;
        }
}

