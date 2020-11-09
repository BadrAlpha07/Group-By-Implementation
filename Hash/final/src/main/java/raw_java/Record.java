package raw_java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A record for out database!
 */
public class Record {

    Integer[] data;

    Record(Integer[] data){
        this.data = data;
    }
    static Record fromString(String line){
        Integer[] data = Arrays.stream(line.split(";")).map(Integer::valueOf).toArray(Integer[]::new);
        return new Record(data);
    }

    public static Record[] fromArray(ArrayList<String> array){
        int n = array.size();
        Record[] records = new Record[n];
        for (int i =0;i<n;i++){
            Record record = fromString(array.get(i));
            records[i] = record;
        }
        return records;
    }
    @Override
    public String toString() {
        String arraystring =  Arrays.toString(data);
        return arraystring.substring(1, arraystring.length()-1);
    }

    Integer get(int i){
        return data[i];
    }

    public void set(int i, Integer v) {
        this.data[i] = v;
    }
    public Record copy(){
        return new Record(this.data);
    }
}
