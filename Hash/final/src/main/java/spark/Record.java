package spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A record for out database!
 */
public class Record implements Serializable {

    String[] data;

    Record(String[] data){
        this.data = data;
    }

    static Record fromString(String line){
        String[] data = (String[]) Arrays.stream(line.split(";")).toArray();
        return new Record(data);
    }

    public static Record[] fromArray(List<ArrayList<String>> array){
        int n = array.size();
        Record[] records = new Record[n];
        for (int i =0;i<n;i++){
            Record record = new Record(array.get(i).toArray(new String[0]));
            records[i] = record;
        }
        return records;
    }

    @Override
    public String toString() {
        String arraystring =  Arrays.toString(data);
        return arraystring.substring(1, arraystring.length()-1);
    }

    public void set(int i, String v) {
        this.data[i] = v;
    }

    public String get(int i) {
        return this.data[i];
    }

    public Record copy(){
        return new Record(this.data);
    }

}