package spark;

/**
 * A Group-By Operator.
 */
public interface GroupBy {

    /*
     * @param input the inputs records.
     * @return output records: the grouped ones.
     */
    Record[] apply(Record[] input);

}