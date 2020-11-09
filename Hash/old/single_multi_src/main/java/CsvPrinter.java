import java.io.*;
import java.util.ArrayList;

public class CsvPrinter {
    static void write(Record[] input, String file) throws IOException {
        var writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
        for(var r: input) {
            writer.println(r);
        }
        writer.flush();
        writer.close();
    }

    static Record[] read(String file) throws IOException {
        var output = new ArrayList<Record>();
        var reader = new BufferedReader(new FileReader(file));
        var line = reader.readLine();
        while(line != null) {
            output.add(Record.fromString(line));
            line = reader.readLine();
        }
        return output.toArray(new Record[0]);
    }

}
