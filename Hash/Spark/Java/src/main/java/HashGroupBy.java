import java.util.HashMap;

/**
 * A group-by operator.
 * Internally uses a hash table.
 */
public class HashGroupBy implements GroupBy{

    private final int in_group;
    private final int in_agg;
    private final int out_group;
    private final int out_agg;
    //private HashMap<Integer,Record> map;
    private CustomHashMap map;
    private Aggregation agg;

    HashGroupBy(int in_group, int in_agg, int out_group, int out_agg, Aggregation agg)
    {
        //this.map = new HashMap<Integer, Record>();
        this.map = new CustomHashMap(0.7f);
        this.in_group = in_group;
        this.in_agg = in_agg;
        this.out_agg = out_agg;
        this.out_group = out_group;
        this.agg = agg;
    }

    public Record[] apply(Record[] input) {
        int output_length = 0;
        for(Record t: input){
            String group = t.get(in_group);
            Record group_val = map.get(group);
            System.out.println(group_val);
            if(group_val == null) {
                map.put(group, agg.initialize(t, in_group, in_agg, out_group, out_agg));
                output_length += 1;
            } else {
                map.put(group, agg.merge(t, group_val,
                        in_group, in_agg, out_group, out_agg));
            }
        }

        return map.values();
    }

    public void set_aggregation(Aggregation agg) {
        this.agg = agg;
    }
}
