/**
 * A multi-processors group-by implementation.
 * Input is hash-partitionned on the group value.
 * So that a group belongs to one and only one partition.
 * Then each partition is dealt with concurrently by a different thread.
 *
 * Compared to other multi-processing scheme, this one has a really low additional cost:
 * One hash by input. Since no groups or data structure are shared between partitions, no addditionnal
 * synchronization or merging strategy has to be implemented.
 *
 * however, if the groups are not uniformly distributed, one thread will have way more job than the others, and will lagg
 * behind. (Not that this will a non-uniform distribution also affects the shared map badly, since threads will be fighting
 * for the same locks. This is not that bad, Since this is not blocking: you can start outputing the others finished grouping
 * as soon as the thread is done.
 *
 *
 * The partition group-by can also be seen as another take on the ConcurrentHashMap, which is closer to the actual
 * java collection implementation. Indeed, this ConcurrentHashMap does not have a lock for each bucket, has lock for groups
 * of buckets, reducing the number of lock-acquering one has to do. In this case, one migh think of a group of buckets sharing
 * the same lock as a partition in our scheme.
 *
 * Mean Cost per input: 1 modulus operation(=f) + (single_processor_cost(=c)/n_threads)
 * Expected speed-up: (n)/((fn/c)+1))
 * In my case, n = 4.
 * I expect f/c to be around 1/10 to 1/1000. Speedup should be close to 3,8.
 *
 * However, we still have to account for thread creation cost, and output copying.
 *
 * thread creation cost is really high: around 10ms.
 *
 * In practice, I end up with around a 1.5 speed-up. Which indicates a f/c ratio of around 0.4.
 *
 *
 *
 *
 */
public class PartitioningHashGroupBy {

    private final int in_group;
    private final int in_agg;
    private final int out_agg;
    private final int out_group;
    private final Aggregation agg;
    private final Thread[] threads;
    private final CustomHashMap[] partitions;
    public long thread_creation_time;
    private Record[] input;

    PartitioningHashGroupBy(int in_group, int in_agg, int out_group, int out_agg, Aggregation agg, int parallel_lvl)
    {
        //this.map = new HashMap<Integer, Record>();
        this.in_group = in_group;
        this.in_agg = in_agg;
        this.out_agg = out_agg;
        this.out_group = out_group;
        this.agg = agg;
        this.partitions = new CustomHashMap[parallel_lvl];
        this.threads = new Thread[parallel_lvl];

    }

    Record[] apply(Record input[]) {
        this.input = input;

        long t1 = System.nanoTime();
        for(int i = 0; i < this.partitions.length; ++i) {
            this.partitions[i] = new CustomHashMap((float) 0.7);
            this.threads[i] = new Thread(new MapFiller(i, this.partitions.length));
            this.threads[i].start();
        }
        long t2 = System.nanoTime();
        long timing = (t2-t1)/(1000000);
        this.thread_creation_time = timing;

        //in this implementation, apply is a blocking operation.
        //if we were doing a iterator interface,
        //we whould not have to wait for each thread to finish.
        for(int i = 0; i < this.threads.length; ++i) {
            try {
                this.threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //because we return one full array instead of an iterator over the ouput value, we
        //have to copy each ouput into a new full size array, which is unfortunate.
        Record[][] outputs = new Record[this.partitions.length][];
        var full_size = 0;
        for(int i = 0; i < this.threads.length; ++i) {
            outputs[i] = this.partitions[i].values();
            full_size += outputs[i].length;
        }
        var output = new Record[full_size];
        var current_offset = 0;
        for(int i = 0; i < this.threads.length; ++i) {
            System.arraycopy(outputs[i], 0, output, current_offset, outputs[i].length);
            current_offset+=outputs[i].length;
        }
        return output;
    }

    /**
     * A runnable that inputs tuple from the input only if they belong to its partition.
     */
    private class MapFiller implements Runnable {

        private final int partition_no;
        private final int n_partitions;

        public MapFiller(int i, int j) {
            this.partition_no = i;
            this.n_partitions = j;
        }

        @Override
        public void run() {
            for(var t: input) {
                var group = t.get(in_group);
                if(group % n_partitions == partition_no) { //modulus partitioning.
                    var group_val = partitions[partition_no].get(group);
                    if(group_val == null) {
                        partitions[partition_no].put(group, agg.initialize(t, in_group, in_agg, out_group, out_agg));
                    } else {
                        partitions[partition_no].put(group, agg.merge(t, group_val,
                                in_group, in_agg, out_group, out_agg));
                    }
                }
            }
        }
    }
}
