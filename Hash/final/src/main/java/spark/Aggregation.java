package spark;

/**
 * An aggregation function.
 */
public interface Aggregation {

    /**
     * @param input     spark.Record from the input.
     * @param group_val spark.Record of the corresponding group.
     * @return The new value for the group record.
     */

    Record merge(Record input,Record group_val, int col_by, int agg_on);
    /**
     * @param input spark.Record from the input.
     *
     *
     * @return A new record corresponding the initial value of the input group.
     */
    Record initialize(Record input,String group, int col_by, int agg_on);

}