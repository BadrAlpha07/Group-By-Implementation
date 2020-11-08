package spark;

/**
 * An aggregation function.
 */
public interface Aggregation {

    public int getAgg_on();
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

    CustomHashMap mergeTables(CustomHashMap t1, CustomHashMap t2);

}