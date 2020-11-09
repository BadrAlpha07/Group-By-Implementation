import java.util.HashMap;

/**
 * A group-by operator using hash table.
 * This parallel implementation is based
 * a partitionning model.
 * Each part gets its own tread and it own hash table.
 * The results are merged together.
 *
 */
public class ParallelHashGroupBy  {

    private final Aggregation agg;
    private final int thread_no;
    private final Thread[] threads;
    private final HashGroupBy[] hgbs;
    private final int in_group;
    private final int in_agg;
    private final int out_group;
    private final int out_agg;

    private class HashGroupBy implements Runnable{

        private final int in_group;
        private final int out_agg;
        private final int out_group;
        private final int in_agg;
        private Record[] input;

        private final int offset;
        private final int stride;
        //private HashMap<Integer,Record> map;
        private CustomHashMap map;
        private Aggregation agg;

        HashGroupBy(int in_group,int in_agg, int out_group, int out_agg, Aggregation agg, Record[] input, int offset, int stride)
        {
            //this.map = new HashMap<Integer, Record>();
            this.map = new CustomHashMap(0.7f);
            this.in_group = in_group;
            this.in_agg = in_agg;
            this.out_group = out_group;
            this.out_agg = out_agg;
            this.agg = agg;
            this.input = input;
            this.offset = offset;
            this.stride = stride;
        }

        public Record[] output() {
            return map.values();
        }

        public void merge(HashGroupBy b)
        {

            int output_length = 0;
            for(var v: b.map.values()){
                var group = v.get(out_group);
                var group_val = map.get(group);
                if(group_val == null) {
                    map.put(group, agg.initialize(v, out_group, out_agg, out_group, out_agg));
                    output_length += 1;
                } else {
                    map.put(group, agg.merge(v, group_val, out_group, out_agg, out_group, out_agg));
                }
            }
        }



        //fills in the hash-table.
        public void run() {
            int output_length = 0;
            for(int i = offset; i<input.length; i+=stride){
                var group = input[i].get(in_group);
                var group_val = map.get(group);
                if(group_val == null) {
                    map.put(group, agg.initialize(input[i], in_group, in_agg, out_group, out_agg));
                    output_length += 1;
                } else {
                    map.put(group, agg.merge(input[i], group_val, in_group, in_agg, out_group, out_agg));
                }
            }
        }

        public void set_aggregation(Aggregation agg) {
            this.agg = agg;
        }

    }


    ParallelHashGroupBy(int in_group, int in_agg, int out_group, int out_agg, Aggregation agg, int threads_no)
    {
        this.agg = agg;
        this.thread_no = threads_no;
        this.threads = new Thread[threads_no];
        this.hgbs = new HashGroupBy[threads_no];
        this.in_group = in_group;
        this.in_agg = in_agg;
        this.out_group = out_group;
        this.out_agg = out_agg;

    }

    public Record[] apply(Record[] input){
        for(int i = 0; i < thread_no; ++i) {
            hgbs[i] = new HashGroupBy(in_group, in_agg, out_group, out_agg, agg, input, i, thread_no);
            threads[i] = new Thread(hgbs[i]);
        }
        //launch each thread.
        for(int i = 0; i < thread_no; ++i) {
            threads[i].run();
        }

        //we now have approximately Nthreads * Ngroups records to group again.
        //it should be way less that Ninput most of the times.
        //we also assume here that each thread sees the same workload
        //so that we do not have a slow worker problem.
        //I don't see why this whould happen since each thread does basically the same thing
        //on the same amount of data.
        //possibly some skew on the data might make it hard on a thread.
        for(int i = 0; i < thread_no; ++i) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //and just append everything to the first hash table.
        //we can just repeat apply() since the hash table is kept in each HashGroupBy object.
        for(int i = 1; i < thread_no; ++i) {
            hgbs[0].merge(hgbs[i]);
        }

        return hgbs[0].output();
    }

}
