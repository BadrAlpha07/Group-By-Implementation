package spark;

import java.io.Serializable;
import java.util.Arrays;

public class CountAggregation implements  Aggregation, Serializable {
    int agg_on;
    int col_by;
    int agg_out = 1;
    int col_out = 0;

    public CountAggregation(int agg_on, int col_by){
        this.agg_on = agg_on;
        this.col_by = col_by;

    }

    @Override
    public int getAgg_on() {
        return agg_on;
    }

    @Override
    public int getCol_by() {
        return col_by;
    }

    @Override
    public Record merge(Record input,Record group_val) {
        group_val.set(this.agg_out, Integer.toString(Integer.parseInt(group_val.get(this.agg_out))+1));
        return group_val;
    }

    @Override
    public Record initialize(Record input,String group){
        return new Record(new String[]{group,"1"});
    }

    @Override
    public CustomHashMap mergeTables(CustomHashMap t1,CustomHashMap t2){
        for (CustomHashMap.HashMapEntry bucket : t2.buckets){
            if (bucket != null) {
                String key = bucket.key;
                Record val1 = t1.get(key);
                if (val1 == null) {
                    val1 = new Record(new String[]{key, "0"});
                }
                Record val2 = t2.get(key);
                val1.set(agg_out, String.valueOf(Integer.parseInt(val1.get(this.agg_out)) + Integer.parseInt(val2.get(this.agg_out))));
            }
        }
        return t1;
    }
}