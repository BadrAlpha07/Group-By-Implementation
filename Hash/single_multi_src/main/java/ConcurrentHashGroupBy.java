import java.util.HashMap;

/**
 * A group-by operator.
 * Internally uses a hash table.
 */
public class ConcurrentHashGroupBy implements GroupBy{

    private final int in_group;
    private final int in_agg;
    private final int out_group;
    private final int out_agg;
    private final Thread[] threads;
    //private HashMap<Integer,Record> map;
    private CustomConcurrentHashMap map;
    private Aggregation agg;
    private Record[] input;

    ConcurrentHashGroupBy(int in_group, int in_agg, int out_group, int out_agg, Aggregation agg, int parallel_lvl)
    {
        //this.map = new HashMap<Integer, Record>();
        this.map = new CustomConcurrentHashMap(64, 0.7f);
        this.in_group = in_group;
        this.in_agg = in_agg;
        this.out_agg = out_agg;
        this.out_group = out_group;
        this.agg = agg;
        this.threads = new Thread[parallel_lvl];
    }

    public Record[] apply(Record[] input) {


        this.input = input;
        for(int i = 0; i < this.threads.length; ++i){
            this.threads[i] = new Thread(new MapFiller(i, this.threads.length));
            this.threads[i].start();
        }
        for(int i = 0; i < this.threads.length; ++i){
            try {
                this.threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return map.values();
    }

    public void set_aggregation(Aggregation agg) {
        this.agg = agg;
    }

    private class MapFiller implements Runnable {
        private final int stride;
        private final int offset;

        public MapFiller(int i, int length) {
            this.offset = i;
            this.stride = length;
        }

        @Override
        public void run() {
            for(int i = offset; i < input.length; i += stride) {
                map.put(input[i].get(in_group), input[i], agg, in_group, in_agg, out_group, out_agg);
            }
        }
    }
}
