import java.util.HashMap;

/**
 * A group-by operator.
 * Internally uses a hash table.
 */
public class HashGroupBy implements GroupBy{

    private final int in_group;
    private HashMap<Integer,Record> map;
    private Aggregation agg;

    HashGroupBy(int group_field, CountAggregation agg)
    {
        this.map = new HashMap<Integer, Record>();
        this.in_group = group_field;
        this.agg = agg;
    }

    public Record[] apply(Record[] input) {
        int output_length = 0;
        for(var t: input){
            var group = t.get(in_group);
            var group_val = map.get(group);
            if(group_val == null) {
                map.put(group, agg.initialize(t));
                output_length += 1;
            } else {
                map.put(group, agg.merge(t, group_val));
            }
        }

        return map.values().toArray(new Record[0]);
    }

    public void set_aggregation(Aggregation agg) {
        this.agg = agg;
    }
}
