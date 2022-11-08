import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class DataSetP_small {

    static final int minValue = 1;
    static final int maxValue = 100;

    public static List<Integer> generateX(){

        // Only generating 50 X values
        ArrayList<Integer> x = new ArrayList<Integer>();
        for(int i = 0; i < 50; i++){
            x.add((int) (Math.random() * (maxValue+1 - minValue) + minValue));
        }
        return x;
    }

    public static List<Integer> generateY(){

        // Only generating 50 Y values
        ArrayList<Integer> y = new ArrayList<Integer>();
        for(int i = 0; i < 50; i++){
            y.add((int) (Math.random() * (maxValue+1 - minValue) + minValue));
        }
        return y;
    }

    public static void generateFile() throws Exception {

        File file = new File("DataSetP_small.csv");
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);

        // There's no need for adding points to csv 300x, we want to keep size low in a testing dataset
        for (int i = 0; i < 1; i++) {
            List<Integer> x = generateX();
            List<Integer> y = generateY();
            for(int j = 0; j < x.size(); j++){
                bw.write(x.get(j) + "," + y.get(j));
                bw.newLine();
            }

        }

        bw.close();
        fw.close();

    }
}
