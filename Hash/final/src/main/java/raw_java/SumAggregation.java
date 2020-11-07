package raw_java;

public class SumAggregation implements Aggregation {


    @Override
    public Record merge(Record input, Record group, int icg, int ica, int gcg, int gca) {
        group.set(gca, group.get(gca) + input.get(ica));
        return group;
    }

    @Override
    public Record initialize(Record input, int in_group, int in_agg, int out_group, int outAgg) {
        return new Record(new Integer[]{input.get(in_agg), input.get(in_group)});
    }
}
