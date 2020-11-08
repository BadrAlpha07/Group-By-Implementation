package spark;

import java.io.Serializable;

/**
 * A custom hash map to support the backend of the spark.HashGroupBy alogrithm.
 * Is supposed to only work with interger keys EDIT : changed to work with any String
 * Uses modular hash for additional simplicity EDIT : uses hashCode() and then modulo.
 * Uses table of power2 size to use the bit hashing trick.
 * Uses a simple open-addressing with linear probing scheme.
 *
 */

public class CustomHashMap{

    public HashMapEntry[] buckets;
    private int load;
    private final float max_fill;


    public static class HashMapEntry{
        String key;
        Record val;
        HashMapEntry(String key, Record val){
            this.key = key;
            this.val = val;
        }
    }


    CustomHashMap(float fill_factor){
        this.max_fill = fill_factor;
        this.load = 0;
        int size = 64;
        this.buckets = new HashMapEntry[size];
    }

    /**
     * @param key String valued key.
     * Hashes the key so that it fits in the buckets.
     *#righ now is simply modular hash
     * Maybe one day we will see a better hashing function used here.#
     */
    private int hash(String key){
        return (key.hashCode() & 0xfffffff)% buckets.length;
    }

    /**
     * @param key the key to probe.
     * @return null if key is not there, the corresponding record otherwise.
     */
    Record get(String key){
        int hash = hash(key);
        HashMapEntry entry = this.buckets[hash];

        while(entry != null && !entry.key.equals(key)) {
            hash = (hash + 1) % this.buckets.length; //linear probing.
            entry = this.buckets[hash];
        }

        // never seen case
        if(entry == null){
            return null;
        } //value is present case
        else {
            return entry.val;
        }

    }


    /**
     * @param key where to put
     * @param val what to put
     * updates or initializes the value at key.
     * If there is not enough place, it might cause a re-structuring of the map.
     */
    void put(String key, Record val){

        int hash = hash(key);
        HashMapEntry entry = this.buckets[hash];
        while(entry != null && !entry.key.equals(key)) {
            hash = (hash+1)%this.buckets.length; //linear probing.
            entry = this.buckets[hash];
        }
        //update case
        if(entry != null) {
            this.buckets[hash].val = val;
        } //adding new value case
        else {

            if((float) this.load/this.buckets.length > this.max_fill) {
                restructure();
                put(key, val);
            } else {
                this.load += 1;
                this.buckets[hash] = new HashMapEntry(key, val);
            }
        }

    }

    private void restructure() {
        int new_size = this.buckets.length * 2;
        HashMapEntry[] old_map = this.buckets.clone();
        this.buckets =  new HashMapEntry[new_size];
        this.load = 0;
        for(HashMapEntry entry: old_map){
            if(entry != null) {
                put(entry.key, entry.val.copy());
            }
        }
    }

    /**
     * @return an array representation of all the records the map contains.
     */
    public Record[] values() {
        Record[] output = new Record[this.load];
        int j = 0;
        for (HashMapEntry bucket : this.buckets) {
            if (bucket != null) {
                output[j] = bucket.val.copy();
                j += 1;
            }
        }
        return output;
    }

}