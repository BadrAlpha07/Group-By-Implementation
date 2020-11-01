import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

class Main{

    private static List<String> parseRecord(List<String> record){
        record = record.stream().map(s -> s.replace("\"","")).collect(Collectors.toList());
        return record;
    }

    private static List<List<String>> readRecords(String fileName){
        List<List<String>> records = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                List<String> valuesList = Arrays.asList(values);
                valuesList = parseRecord(valuesList);
                records.add(valuesList);
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public static void add(String key, List<String> record, Hashtable<String, List<List<String>>> multiMap) {
        List<List<String>> list;
        if (multiMap.containsKey(key)) {
            list = multiMap.get(key);
            list.add(record);
        } else {
            list = new ArrayList<>();
            list.add(record);
            multiMap.put(key, list);
        }
    }


    public static int aggregateCount(List<List<String>> records){
        return records.size();
    }

    public static void main(String[] args){
        Hashtable<String, List<List<String>>>
                hm = new Hashtable<String, List<List<String>>>();
        List<List<String>> records = readRecords("Hash/data_test1.csv");
        System.out.println(records);
        int n = records.size();
        int m = records.get(0).size();
        List<String> headRow = records.get(0);
        System.out.println(String.format("%d rows, %d columns : %s ",n,m,headRow));

        for (int i = 1;i<n;i++){
            List<String> record = records.get(i);
            System.out.println(record);
            add(record.get(1),record,hm);

        }
        Enumeration<String> keys = hm.keys();
        System.out.println("Aggregation Results");
        for (String key:Collections.list(keys)){
            List<String> agg = Arrays.asList(key,Integer.toString(aggregateCount(hm.get(key))));
            System.out.println(agg);


        }
    }

}

