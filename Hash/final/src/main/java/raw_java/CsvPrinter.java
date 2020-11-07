package raw_java;

import java.io.*;
import java.util.ArrayList;

public class CsvPrinter {
    static void write(Record[] input, String file) throws IOException {
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
        for(Record r: input) {
            writer.println(r);
        }
        writer.flush();
        writer.close();
    }

    static Record[] read(String file) throws IOException {
        ArrayList<Record> output = new ArrayList<Record>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        while(line != null) {
            output.add(Record.fromString(line));
            line = reader.readLine();
        }
        return output.toArray(new Record[0]);
    }

}
