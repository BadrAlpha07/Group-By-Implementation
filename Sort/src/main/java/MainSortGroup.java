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

public class MainSortGroup {
       public static void main(String[] args) throws FileNotFoundException, InterruptedException, ExecutionException {
        JFileChooser chooser = new JFileChooser();
       MainSortGroup s = new MainSortGroup(); 
       File workingDirectory = new File(System.getProperty("user.dir"));
       chooser.setCurrentDirectory(workingDirectory);
       int status = chooser.showOpenDialog(null);
       if (status == JFileChooser.APPROVE_OPTION) {
            File file = chooser.getSelectedFile();
            if (file == null) {
                return;
            }
       String path = chooser.getSelectedFile().getAbsolutePath();
       Read_CSV read = new Read_CSV();
       String[][] matrix,matrix_ghost;
       matrix_ghost=read.read_csv(path,0);
       int maxPosition = matrix_ghost[0].length ;
       String col="Select your column name";
       String[] cols = new String[maxPosition+1]; 
       cols[0]=col;
       for (int i=0;i<maxPosition;i++) 
       {   
           cols[i+1]=matrix_ghost[0][i];
       }
       matrix=read.read_csv(path,1);

       String[] choices = new String[] {"Select your implementation mode","Single threaded", "Multi-threaded",
                                    "Spark"};
    JFrame frame = new JFrame("Editable JComboBox");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    final JComboBox comboBox = new JComboBox(choices);
    final JComboBox comboBox1 = new JComboBox(cols);
    frame.add(comboBox, BorderLayout.SOUTH);
    frame.add(comboBox1, BorderLayout.NORTH);
   
   ActionListener actionListener = new ActionListener() {
      public void actionPerformed(ActionEvent actionEvent) {
        String choice= (String) comboBox.getSelectedItem();
        int col_index= comboBox1.getSelectedIndex()-1;
        frame.setVisible(false);
        if (choice.equals(choices[1]))
        {   System.out.println("Column: " + col_index);
            System.out.println("Selected: " + choice);
            SingleThreaded single= new SingleThreaded();
  try {
                single.SingleThreadSort(matrix,col_index);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(MainSortGroup.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else if (choice.equals(choices[2]))
        {   System.out.println("Column: " + col_index);
            System.out.println("Selected: " + choice);
            MultiThreaded mutli= new MultiThreaded();
            try {
                mutli.GroupingMulti(matrix,4,col_index);
            } catch (InterruptedException ex) {
                Logger.getLogger(MainSortGroup.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ExecutionException ex) {
                Logger.getLogger(MainSortGroup.class.getName()).log(Level.SEVERE, null, ex);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(MainSortGroup.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else if (choice.equals(choices[3])) {
            System.out.println("Column: " + col_index);
            System.out.println("Selected: " + choice);
            GroupbySortSpark spark= new GroupbySortSpark();
            SparkConf conf = new SparkConf();
            conf.setAppName("Application_name").setMaster("local[*]");
            JavaSparkContext sc = null;
            sc = new JavaSparkContext(conf);
            int nbThreads=4;
            try {
                spark.Spark_grouby(path,nbThreads,col_index,sc);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(MainSortGroup.class.getName()).log(Level.SEVERE, null, ex);
            } 
        }
        System.exit(0);
      }
    };
   comboBox.addActionListener(actionListener);
    frame.setSize(300, 200);
    frame.setVisible(true);
    }
}
}
