package spark;
import java.io.Serializable;
import java.util.Arrays;


/*The CountAggregation implementation
 */
public class CountAggregation implements  Aggregation, Serializable {
    int agg_on;
    int col_by;
    int agg_out = 1; // the column of the result of the aggregation (here default is 1)
    int col_out = 0;  // the column of the grouping value (here default is 0)
    // -> the ouptut format will be grouping_value;aggregation_value

    
     /**
     *
     * @param agg_on agg_on the index of the column on which we are aggregating.
     * @param col_by the index of the column on which we are groupping by.
     */
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
    
    
    /**
     *
     * @param input     spark.Record from the input.
     * @param group_val spark.Record of the corresponding group.
     * @return the row resulting of the merge in the format grouping_value;aggregation_value
     */
    @Override
    public Record merge(Record input,Record group_val) {
        group_val.set(this.agg_out, Integer.toString(Integer.parseInt(group_val.get(this.agg_out))+1));
        return group_val;
    }

    
     /**
     *
     * @param input spark.Record from the input.
     * @param group the grouping value
     * @return the row resulting of the initialization
     */
    @Override
    public Record initialize(Record input,String group){
        return new Record(new String[]{group,"1"});
    }
    
    
    /**
     *
     * @param t1 a hashtable resulting of the early aggregation on one node
     * @param t2 ---------------------------------------------- on another node
     * @return t1 + t2, merged key by key
     */
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
