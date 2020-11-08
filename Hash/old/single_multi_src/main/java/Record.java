import java.util.Arrays;

/**
 * A record for out database!
 */
public class Record {

    Integer[] data;

    Record(Integer[] data){
        this.data = data;
    }
    static Record fromString(String line){
        var data = Arrays.stream(line.split(",")).map(Integer::valueOf).toArray(Integer[]::new);
        return new Record(data);
    }

    @Override
    public String toString() {
        var arraystring =  Arrays.toString(data);
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
