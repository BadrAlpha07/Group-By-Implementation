public class CountAggregation implements Aggregation{
    private final int input_col;
    private final int output_col;
    private final int input_group_col;


    public CountAggregation(int input_agg_col, int input_group_col){
        this.input_col = input_agg_col;
        this.input_group_col = input_group_col;
        this.output_col = 0;
    }
    public Record merge(Record input, Record group) {
        group.set(output_col, group.get(output_col)+1);
        return group;
    }

    @Override
    public Record initialize(Record input) {
        return new Record(new Integer[]{1, input.get(input_group_col)});
    }
}
