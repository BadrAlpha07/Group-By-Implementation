import java.io.IOException;
import java.util.Random;

public class main {

    public static void main(String[] args) {

        var input_size = 8000000;//this is about the maximum input size I can handle on my computer.
        var outout_size = 30000;
        var input = new Record[input_size];
        var random = new Random();
        var groups = new int[outout_size];
        for(int i = 0; i < outout_size; ++i){
            groups[i] = random.nextInt(input_size);
        }
        for(int i = 0; i < input_size; ++i){
            input[i] = new Record(new Integer[]{1, 2, 3, groups[random.nextInt(outout_size)]});

        }
        /*
        try {
            CsvPrinter.write(input, "test.data");
        } catch (IOException e){e.printStackTrace();};
        */
        var group_col = 3;
        Record[] output = null;
        long t1 = System.nanoTime();
        for(int i = 0; i  < 10; ++i) {
            var agg = new SumAggregation();
            var grp = new HashGroupBy(group_col,0, 1, 0, agg);
            output = grp.apply(input);
        }
        long t2 = System.nanoTime();
        long timing = (t2-t1)/(1000000*10);
        System.out.format("took %d ms\n", timing);
        try {
           CsvPrinter.write(output, "test_out_single.data");
        } catch (IOException e) {
            e.printStackTrace();
        }



        t1 = System.nanoTime();
        var parallel_lvl = Runtime.getRuntime().availableProcessors();
        long thread_creation_cost = 0;
        for(int i = 0; i  < 10; ++i) {
            var agg = new SumAggregation();
            var grp = new PartitioningHashGroupBy(group_col,0, 1, 0, agg, 4);
            output = grp.apply(input);
            thread_creation_cost = grp.thread_creation_time;
        }
        t2 = System.nanoTime();
        timing = (t2-t1)/(1000000*10);
        System.out.format("took %d ms on %d processors\n", timing, parallel_lvl);
        System.out.format("of which %d ms where spent creating the threads.\n", thread_creation_cost);
        try {
            CsvPrinter.write(output, "test_out_multi.data");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }



}

