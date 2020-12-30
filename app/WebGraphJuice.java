import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class WebGraphJuice {

    public static void main(String[] args) throws Exception {
        String key = args[0];
        String filePath = args[1];
        int count = 0;


        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.length() > 0) {
                    ++count;
                }
            }

            reader.close();
            System.out.println(key + "," + count);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
