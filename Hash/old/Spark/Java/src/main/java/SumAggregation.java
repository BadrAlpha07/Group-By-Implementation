import java.io.Serializable;

public class SumAggregation implements Aggregation, Serializable {


    @Override
    public Record merge(Record input, Record group_val, int col_by, int agg_on) {
        group_val.set(1, String.valueOf(Float.parseFloat(group_val.get(1))+Float.parseFloat(input.get(agg_on))));
        return group_val;
    }


    @Override
    public Record initialize(Record input, String group, int col_by, int agg_on) {
        return new Record(new String[]{group,input.get(agg_on)});
    }
}
