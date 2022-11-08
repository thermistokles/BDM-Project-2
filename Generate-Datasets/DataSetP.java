import jdk.jshell.execution.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataSetP {

    static final int minValue = 1;
    static final int maxValue = 10000;

    public static List<Integer> generateX(){

        ArrayList<Integer> x = new ArrayList<Integer>();
        for(int i = 0; i < 50000; i++){
            x.add((int) (Math.random() * (maxValue+1 - minValue) + minValue));
        }
        return x;
    }

    public static List<Integer> generateY(){

        ArrayList<Integer> y = new ArrayList<Integer>();
        for(int i = 0; i < 50000; i++){
            y.add((int) (Math.random() * (maxValue+1 - minValue) + minValue));
        }
        return y;
    }

    public static void generateFile() throws Exception {

        File file = new File("DataSetP.csv");
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);

        for (int i = 0; i < 300; i++) {
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
