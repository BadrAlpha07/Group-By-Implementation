
/**
 * A group-by operator.
 * Internally uses a hash table.
 */
public class HashGroupBy implements GroupBy{


    private final int by;
    private final int agg_on;
    private final CustomHashMap map;
    private final Aggregation agg;

    HashGroupBy(int by, int agg_on, Aggregation agg)
    {
        //this.map = new HashMap<Integer, Record>();
        this.map = new CustomHashMap(0.7f);
        this.by = by;
        this.agg_on = agg_on;
        this.agg = agg;
    }

    public Record[] apply(Record[] input) {
        for(Record record: input){
            String group = record.get(by);
            Record group_val = map.get(group);

            if(group_val == null) {
                map.put(group, agg.initialize(record,group,by,agg_on));
            } else map.put(group, agg.merge(record, group_val, by, agg_on));
        }

        return map.values();
    }

}
