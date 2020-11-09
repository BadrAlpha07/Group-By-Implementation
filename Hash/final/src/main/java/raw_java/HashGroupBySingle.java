package raw_java;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

/**
 * A group-by operator.
 * Internally uses a hash table.
 */
public class HashGroupBySingle implements GroupBy{

    private final int in_group;
    private final int in_agg;
    private final int out_group;
    private final int out_agg;
    //private HashMap<Integer,Record> map;
    private CustomHashMap map;
    private Aggregation agg;

    public HashGroupBySingle(int in_group, int in_agg, int out_group, int out_agg, Aggregation agg)
    {
        //this.map = new HashMap<Integer, Record>();
        this.map = new CustomHashMap(0.7f);
        this.in_group = in_group;
        this.in_agg = in_agg;
        this.out_agg = out_agg;
        this.out_group = out_group;
        this.agg = agg;
    }

    public Record[] apply(String file_path) {
        int output_length = 0;
        try {
            File myObj = new File(file_path);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                Record t = Record.fromString(data);
                int group = t.get(in_group);
                Record group_val = map.get(group);
                if(group_val == null) {
                    map.put(group, agg.initialize(t, in_group, in_agg, out_group, out_agg));
                    output_length += 1;
                } else {
                    map.put(group, agg.merge(t, group_val,
                            in_group, in_agg, out_group, out_agg));
                }
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return map.values();
    }

    public void set_aggregation(Aggregation agg) {
        this.agg = agg;
    }
}
