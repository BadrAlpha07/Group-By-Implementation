package spark;

/**
 * A group-by operator.
 * Internally uses a hash table.
 */
public class HashGroupBy implements GroupBy{


    private final int col_by;
    private final int agg_on;
    private final CustomHashMap map;
    private final Aggregation agg;

    HashGroupBy(int col_by, int agg_on, Aggregation agg)
    {
        //this.map = new HashMap<Integer, spark.Record>();
        this.map = new CustomHashMap(0.7f);
        this.col_by = col_by;
        this.agg_on = agg_on;
        this.agg = agg;
    }

    public CustomHashMap apply(Record[] input) {
        for(Record record: input){
            String group = record.get(col_by);
            Record group_val = map.get(group);

            if(group_val == null) {
                map.put(group, agg.initialize(record,group));
            } else map.put(group, agg.merge(record, group_val));
        }

        return map;
    }

}
