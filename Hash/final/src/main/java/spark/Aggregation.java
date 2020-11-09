package spark;

/**
 * An aggregation function.
 * Only CountAggregation is implemented as it is used for test, the other aggregation functions are straightforward.
 */

public interface Aggregation {

    /**
    @return agg_on the index of the column on which we are aggregating.
     **/
    public int getAgg_on();
     
    
    /**
     @return col_by the index of the column on which we are groupping by.
     **/
    public int getCol_by();

    
    /**
     * @param input     spark.Record from the input.
     * @param group_val spark.Record of the corresponding group.
     * @return The new value for the group record.
     */                           
    Record merge(Record input,Record group_val);
    
    
    /**
     * @param input spark.Record from the input.
     *
     *
     * @return A new record corresponding the initial value of the input group.
     */
    Record initialize(Record input,String group);

     /**
     *
     * @param t1 a hashtable resulting of the early aggregation on one node
     * @param t2 ---------------------------------------------- on another node
     * @return t1 + t2, merged key by key
     */
    CustomHashMap mergeTables(CustomHashMap t1, CustomHashMap t2);

}
