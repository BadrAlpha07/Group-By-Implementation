
import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class Main_Test {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, InterruptedException, ExecutionException {
        
        
        final File folder = new File("C:\\home"); //Path
        
        MultiThreaded mutli= new MultiThreaded();
        SingleThreaded single= new SingleThreaded();
        SparkConf conf = new SparkConf();
        conf.setAppName("Application_name").setMaster("local[*]");
        JavaSparkContext sc = null;
        sc = new JavaSparkContext(conf);
        int threadPoolSize=4;
	sc.setLogLevel("WARN");
        
        GroupbySortSpark spark= new GroupbySortSpark();
        //JavaSparkContext sc = null;
        
        WriterFile result_spark = new WriterFile("result_Spark.csv");
        WriterFile result_Multi = new WriterFile("result_Multi.csv");
        WriterFile result_Single = new WriterFile("result_Single.csv");
        int n_test = 100; // number of test 
        float[] temps_spark=new float[n_test];
        for (final File fileEntry : folder.listFiles()){
            String path= fileEntry.getPath();
            System.out.println("Spark"+path);
        
        for (int i=0; i<n_test;i++){ 
            System.out.println(i);
        long t1 = System.nanoTime();
        spark.Spark_grouby(path,4,1,sc);
        long t2 = System.nanoTime(); 
        long timing = (t2-t1)/(1000000);
        temps_spark[i]=timing;
      }
        float cal=0;
        for(int i=0;i<n_test;i++){
            cal=cal+temps_spark[i];
        }
        path = path.replace("\\", "/");
        String[] splittedFileName = path.split("/");
        String Name = splittedFileName[splittedFileName.length-1];
        
        float sum=cal/n_test;
        result_spark.writeLine(Name+";"+Float.toString(sum));
      }
        result_spark.closeFile();

      //____________________________________________________________________________
      float[] temps_Multi=new float[n_test]; 
        for (final File fileEntry : folder.listFiles()){
            String path= fileEntry.getPath();
            System.out.println("Multi"+path);
        Read_CSV read = new Read_CSV();
        String [][] matrix=read.read_csv(path,1);
        for (int i=0; i<n_test;i++){ 
            
        long t3 = System.nanoTime();
        mutli.GroupingMulti(matrix,threadPoolSize,1);
        long t4 = System.nanoTime();
        long timing_2 = (t4-t3)/(1000000);
        temps_Multi[i]=timing_2;
        System.out.println(timing_2);
        
      }
        float cal=0;
        for(int i=0;i<temps_Multi.length;i++){
            cal=cal+temps_Multi[i];
        }
        path = path.replace("\\", "/");
        String[] splittedFileName = path.split("/");
        String Name = splittedFileName[splittedFileName.length-1];
        
        float sum=cal/n_test;
        result_Multi.writeLine(Name+";"+Float.toString(sum));
      }
        result_Multi.closeFile();
        //_________________________________________________________________________
        float[] temps_Single=new float[n_test]; 
        for (final File fileEntry : folder.listFiles()){
            String path= fileEntry.getPath();
            System.out.println("Single"+path);
        
        for (int i=0; i<n_test;i++){ 
        Read_CSV read = new Read_CSV();
        String [][] matrix=read.read_csv(path,1);
        long t5 = System.nanoTime();
        single.SingleThreadSort(matrix,1);
        long t6 = System.nanoTime();
        long timing_3 = (t6-t5)/(1000000);
        temps_Single[i]=timing_3;
        
      }
        float cal=0;
        for(int i=0;i<n_test;i++){
            cal=cal+temps_Single[i];
        }
        
       path = path.replace("\\", "/");
       String[] splittedFileName = path.split("/");
       String Name = splittedFileName[splittedFileName.length-1];
       
        float sum=cal/n_test;
        result_Single.writeLine(Name+";"+Float.toString(sum));
      }
        result_Single.closeFile();
       
    }
    
}
