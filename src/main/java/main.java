import java.io.IOException;
import java.util.Random;

public class main {

    public static void main(String[] args) {

        var input_size = 10000000;//should be around half a Gb of data.
        var outout_size = 100;
        var input = new Record[input_size];
        var random = new Random();
        var groups = new int[outout_size];
        for(int i = 0; i < outout_size; ++i){
            groups[i] = random.nextInt(input_size);
        }
        for(int i = 0; i < input_size; ++i){
            input[i] = new Record(new Integer[]{1, 2, 3, groups[random.nextInt(outout_size)]});

        }
        try {
            CsvPrinter.write(input, "test.data");
        } catch (IOException e){e.printStackTrace();};

        var group_col = 3;
        var agg = new CountAggregation(0,group_col);
        var grp = new HashGroupBy(group_col, agg);
        long t1 = System.nanoTime();
        var output = grp.apply(input);
        long t2 = System.nanoTime();
        long timing = (t2-t1)/1000;
        System.out.format("took %d ms\n\n", timing);
        try {
            CsvPrinter.write(output, "test_out.data");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}

