public class Main {
    public static void main(String[] args) throws Exception {
        String[] input = new String[3];
        String data_size = "small";

        // Setting the 2 input file paths
        if (data_size.equals("small")) {
            input[0] = "hdfs://localhost:9000/proj2/DataSetP_small.csv";
            input[1] = "hdfs://localhost:9000/proj2/DatasetR_small.csv";
        }
        else if (data_size.equals("large")) {
            input[0] = "hdfs://localhost:9000/cs585/DataSetP_small.csv";
            input[1] = "hdfs://localhost:9000/cs585/DatasetR_small.csv";
        }

        // Setting the output file path
        input[2] = "hdfs://localhost:9000/proj2/output.csv";


        // Run 1st MR job
        SpatialJoin.start(input);
    }
}
