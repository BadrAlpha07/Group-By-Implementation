/**
 * A Group-By Operator.
 */
public interface GroupBy {


    /*
     *
     * @param input the inputs records.
     * @return output records: the grouped ones.
     */
    Record[] apply(Record[] input);

    /**
     * Agg specifies how should record belong to the same group be aggregated.
     * @param agg
     */
    void set_aggregation(Aggregation agg);
}
