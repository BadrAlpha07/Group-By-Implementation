/**
 * An aggregation function.
 */
public interface Aggregation {

    /**
     * @param input Record from the input.
     * @param group Record of the corresponding group.
     * @return The new value for the group record.
     */
    Record merge(Record input, Record group, int icg, int ica, int gcg, int gca);
    /**
     * @param input Record from the input.
     * @param out_agg
     * @param in_agg
     * @param out_group
     * @param outAgg
     * @return A new record corresponding the initial value of the input group.
     */
    Record initialize(Record input, int out_agg, int in_agg, int out_group, int outAgg);
}