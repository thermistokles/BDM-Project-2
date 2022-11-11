public class Main {
    public static void main(String[] args) throws Exception {
        /* ----- Project 2 Problem 2 ----- */
        boolean W_included = true;
        String[] input;
        if (W_included){
            input = new String[7];
            input[0] = "hdfs://localhost:9000/proj2/DataSetP.csv";
            input[1] = "hdfs://localhost:9000/proj2/DataSetR.csv";
            input[2] = "hdfs://localhost:9000/proj2/final_output.csv";
            input[3] = "1"; // W - x1
            input[4] = "1"; // W - y1
            input[5] = "200"; // W - x2
            input[6] = "200"; // W - y2

        }
        else {
            input = new String[3];
            input[0] = "hdfs://localhost:9000/proj2/DataSetP_small.csv";
            input[1] = "hdfs://localhost:9000/proj2/DataSetR_small.csv";
            input[2] = "hdfs://localhost:9000/proj2/final_output.csv";
        }

        // Run 1st MR job
        SpatialJoin sj = new SpatialJoin();
        sj.start(input);
    }
}
