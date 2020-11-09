package raw_java;

import java.util.Arrays;
import java.util.HashMap;

/**
 * A custom hash map to support the backend of the HashGroupBy alogrithm.
 * Is supposed to only work with interger keys.
 * Uses modular hash for additional simplicity.
 * Uses table of power2 size to use the bit hashing trick.
 * Uses a simple open-addressing with linear probing scheme.
 *
 */
public class CustomHashMap {


    private HashMapEntry[] buckets;
    private int load;
    private final float max_fill;
    private final int size;


    private class HashMapEntry{
        int key;
        Record val;
        HashMapEntry(int key, Record val){
            this.key = key;
            this.val = val;
        }
    }


    CustomHashMap(float fill_factor){
        this.max_fill = fill_factor;
        this.load = 0;
        this.size = 64;
        this.buckets = new HashMapEntry[this.size];
    }

    /**
     * @param key integer valued key.
     * Hashes the key so that it fits in the buckets.
     * righ now is simply modular hash
     * Maybe one day we will see a better hashing function used here.
     */
    private int hash(int key){
        return key % buckets.length;
    }

    /**
     * @param key the key to probe.
     * @return null if key is not there, the corresponding record otherwise.
     */
    Record get(int key){
        int hash = hash(key);
        HashMapEntry entry = this.buckets[hash];
        while(entry != null && entry.key != key) {
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
    void put(int key, Record val){

        int hash = hash(key);
        HashMapEntry entry = this.buckets[hash];
        while(entry != null && entry.key != key) {
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
    Record[] values() {
        Record[] output = new Record[this.load];
        int j = 0;
        for(int i = 0; i < this.buckets.length; ++i) {
            if(this.buckets[i] != null){
                output[j] = this.buckets[i].val.copy();
                j += 1;
            }
        }
        return output;
    }

}
