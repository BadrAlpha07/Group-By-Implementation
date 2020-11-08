/**
 * A concurrent hash map implementation, by locking each bucket individually.
 * Entry and keys are separated because it is a bit easier to lock them this way.
 * This map only support adding new values and editing an existing value.
 * No removing / testing for existence.
 * The hard thing here is to provide a way to lock out the map
 * when it is restructured.
 *
 *
 */
public class CustomConcurrentHashMap {

    private  Boolean restructuring;
    private Integer[] keys;
    private Record[] vals;
    private Object[] key_locks;
    private Object[] val_locks;
    private int load;
    private final float fill_f;
    private Integer custom_semaphore; //a custom semaphore to ber able to block the map usage when we are restructuring it.
    private Object semaphore_lock;
    //counts the number of actions been performed.

    CustomConcurrentHashMap(int initial_size, float fill_factor) {
        this.keys = new Integer[initial_size];
        this.vals = new Record[initial_size];
        this.key_locks = new Object[initial_size];
        this.val_locks = new Object[initial_size];
        for(int i = 0; i < initial_size; ++i){
            this.key_locks[i] = new Object();
            this.val_locks[i] = new Object();
        }

        this.load = 0;
        this.fill_f = fill_factor;
        this.restructuring = false;
        this.semaphore_lock = new Object();
        this.custom_semaphore = 0;

    }

    /**
     * @param key integer key to be hashed to fit in the map.
     * @return an integer value between [0, map size]
     */
    private int hash(int key){
        return (key)%this.keys.length;
    }

    /**
     * linear probing. Keep on reading buckets until null or same key is found.
     * If a new null bucket is found, it is updated to the new key in a synchronized fashion,
     * to ensure no concurrent probing picks the same slot for a different key.
     * @param key integer value.
     */
    private Integer prob(int key){
        var p = hash(key);
        boolean found_bucket = false;
        while (!found_bucket){
            synchronized (this.key_locks[p]) { //synchronized null check  + write.
                if (this.keys[p] == null) {
                    this.keys[p] = key;
                }
            }
            if (this.keys[p] == key){
                found_bucket = true;
            } else
            {
                p = (p + 1)%this.keys.length;
            }
        }

        return p;
    }


    /**
     * In our case, we want to be able to update record in the map in an atomic fashion.
     * Put provides the way to do that.
     * This it why we need to provide it with the Aggregation and all other information
     * necessary to perform the initialization or update of our value.
     * @param key the key were to add the new value.
     * @param val the new value.
     */
    void put(int key, Record val, Aggregation agg, int in_group, int in_agg, int out_group, int out_agg) {

        //System.out.println(val);
        synchronized (this.semaphore_lock){
            while(this.restructuring) {
                //System.out.println("waiting for restructurign");
                try {
                    this.semaphore_lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            this.custom_semaphore += 1;
            //System.out.format("exiting semaphore lock %d\n", this.custom_semaphore);
        }



        var p = prob(key);
        //System.out.println("entering val lock");
        synchronized (this.val_locks[p]) { //ensure that no updating conflict for this v alue happens.
            //simple case: we are updating.
            if (this.vals[p] != null) {
                this.vals[p] = agg.merge(val, this.vals[p], in_group, in_agg, out_group, out_agg);
            }
            else{
                this.load += 1;
                this.vals[p] = agg.initialize(val, in_group, in_agg, out_group, out_agg);

                if ((float) this.load / this.keys.length > fill_f) {
                    //really hard case: we are restructuring. We need to freeze the map to do that before.
                    //we need to ensure no conflict for the mean time.
                    synchronized (this.semaphore_lock) {
                        while (this.custom_semaphore != 1) {
                            //System.out.println("waiting for free");
                            try {
                                this.semaphore_lock.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        this.restructuring = true;
                        restructure();
                        this.restructuring = false;
                    }
                }
            }
        }






        synchronized (this.semaphore_lock){
            this.custom_semaphore -= 1;
            if(this.custom_semaphore==0){
                semaphore_lock.notifyAll();
            }
        }
    }



    /**
     * @param key the key associated to the queried value.
     * @return null if key is not there. The associated record otherwise.
     * No syncronisation on get. I don't care if you do not get the latest value...
     */
    Record get(int key) {
        var p = prob(key);
        return vals[p];
    }

    Record[] values() {
        var output = new Record[this.load];
        int j = 0;
        for(int i = 0; i < this.keys.length; ++i) {
            if(this.keys[i] != null) {
                output[j] = this.vals[i];
                j+=1;
            }
        }
        return output;
    }

    /**
     * BAD AND SLOW OPERATION: TRY TO AVOID IT.
     * It is not even multithreaded. What a shame.
     */
    private void restructure() {
        System.out.println("reeeeeestructure");
        var new_size = this.keys.length * 2;
        var temp_keys = this.keys.clone();
        var temp_vals = this.vals.clone();
        this.val_locks = new Object[new_size];
        this.key_locks = new Object[new_size];
        for(int i = 0; i < new_size; ++i){
            this.val_locks[i] = new Object();
            this.key_locks[i] = new Object();
        }
        this.vals = new Record[new_size];
        this.keys = new Integer[new_size];
        int j = 0;
        for(int i = 0; i < temp_keys.length; ++i){
            if(temp_keys[i] != null){
                var p = prob(temp_keys[i]);
                vals[p] = temp_vals[i];
            }
        }
    }

}

