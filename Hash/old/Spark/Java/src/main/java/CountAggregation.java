import java.io.Serializable;

public class CountAggregation implements  Aggregation, Serializable {


    @Override
    public Record merge(Record input,Record group_val, int col_by, int agg_on) {
        group_val.set(1, Integer.toString(Integer.parseInt(group_val.get(1))+1));
        return group_val;
    }

    @Override
    public Record initialize(Record input,String group, int col_by, int agg_on){
        return new Record(new String[]{group,"1"});
    }
}