import java.io.IOException;

public class main {

    public static void main(String[] args) throws Exception {
        String data_size = "small";
//        String data_size = "normal";

        /**
         * Normal dataset:
         *      - Space extends from 1 to 10,000 for x and y axes
         *      - a record in P in a point, a record in R is a rectangle
         *      - 50,000 (x,y) pairs
         *      - size should reach 100MB
         *
         * Small dataset:
         *      - Space extends from 1 to 100 for x and y axes
         *      - a record in P in a point, a record in R is a rectangle
         *      - 50 (x,y) pairs in the dataset
         *      - size will be VERY small (can open in excel to view)
         * */

        if (data_size.equals("small")){
            DataSetP_small.generateFile();
            DataSetR_small.generateFile();
        }
        else if (data_size.equals("normal")) {
            DataSetP.generateFile();
            DataSetR.generateFile();
        }
    }
}
