package raw_java;

public class CountAggregation implements Aggregation{
    public final int input_col;
    public final int output_col;
    public final int input_group_col;
    private final int output_group_col;


    public CountAggregation(int input_agg_col, int input_group_col){
        this.input_col = input_agg_col;
        this.input_group_col = input_group_col;
        this.output_col = 0;
        this.output_group_col = 1;
    }
    public Record merge(Record input, Record group) {
        group.set(output_col, group.get(output_col)+1);
        return group;
    }

    @Override
    public Record merge(Record input, Record group, int icg, int ica, int gcg, int gca) {
        return merge(input, group);
    }

    @Override
    public Record initialize(Record input, int icg, int ica, int gcg, int gca ) {
        return new Record(new Integer[]{1, input.get(icg)});
    }
}
