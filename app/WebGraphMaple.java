
public class WebGraphMaple {

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String[] inputLines = input.split("\\r?\\n");

        for (String line : inputLines) {
            if (line.length() > 0) {
                String[] kV  = line.split(",");
                int key = Integer.parseInt(kV[0]), val = Integer.parseInt(kV[1]);
                if (val >= 1 && val <= 3) {
                    System.out.println(val + "," + 1);
                }
            }
        }
    }
}
