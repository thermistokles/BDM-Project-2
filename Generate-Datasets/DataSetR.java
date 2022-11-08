import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class DataSetR {

    static final int minValue = 1;
    static final int minHeight = 1;
    static final int minWidth = 1;
    static final int maxValue = 10000;
    static final int maxHeight = 20;
    static final int maxWidth = 5;


    public static String generateRectangle(){

        //String rectangle = "";

        int x1 = (int) (Math.random() * ((maxValue - maxWidth)+1 - minValue) + minValue);
        int y1 = (int) (Math.random() * ((maxValue - maxHeight)+1 - minValue) + minValue);

        int h = (int) (Math.random() * ((maxHeight)+1 - minHeight) + minHeight);
        int w = (int) (Math.random() * ((maxWidth)+1 - minWidth) + minWidth);

        System.out.println("x1: "+ x1 +" y1: "+ y1);
        System.out.println("h: "+ h +" w: "+ w);

        int x2 = x1 + w;
        int y2 = y1 + h;

        System.out.println("x2: "+ x2 +" y2: "+ y2);

        return String.valueOf(x1) + ',' + String.valueOf(y1) + ',' + String.valueOf(x2) + ',' + String.valueOf(y2);

    }

    public static void generateFile() throws Exception {

        System.out.println("Rectangle: "+ generateRectangle());

        File file = new File("DataSetR.csv");
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);

        for (int i = 0; i < 5000000; i++) {
                bw.write(generateRectangle());
                bw.newLine();

        }

        bw.close();
        fw.close();

    }

}
