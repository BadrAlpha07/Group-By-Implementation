/**
 * An aggregation function.
 */
public interface Aggregation {

    /**
     * @param input     Record from the input.
     * @param group_val Record of the corresponding group.
     * @return The new value for the group record.
     */

    Record merge(Record input,Record group_val, int col_by, int agg_on);
    /**
     * @param input Record from the input.
     *
     *
     * @return A new record corresponding the initial value of the input group.
     */
    Record initialize(Record input,String group, int col_by, int agg_on);

}