import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class SpatialJoin {
    // In this solution, the space will be divided into [x] regions (numbered in LR->TB order).
    private static final int regionXY = 1000; // 5 (small) or 1000 (large)
    private static final int numRegions = regionXY * regionXY; // MUST be square
    private static final int spaceSize = 10000; // X and Y dimension of entire 2D space, 100 (small) or 10000 (large)
    private static final int numPoints = spaceSize * spaceSize; // MUST be square

    public static class PointMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Splitting input line
            String line = value.toString();
            String[] tokens = line.split(",");

            // Getting numerical value from input
            int x1 = Integer.parseInt(tokens[0]);
            int y1 = Integer.parseInt(tokens[1]);

            // Getting the region the point belongs to for KEY, formatting output
            IntWritable region = getRegionNum(x1, y1, numRegions, spaceSize);
            Text output_record = new Text("p-" + x1 + "-" + y1);
            // Outputting KV pair <region, original record value>
            context.write(region, output_record);
        }
    }

    public static class RectMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Splitting input line
            String line = value.toString();
            String[] tokens = line.split(",");

            // Getting numerical value from input
            int x1 = Integer.parseInt(tokens[0]);
            int y1 = Integer.parseInt(tokens[1]);
            int x2 = Integer.parseInt(tokens[2]);
            int y2 = Integer.parseInt(tokens[3]);

            // Getting the regions the point belongs to generate 1-M KEYS, formatting output
            IntWritable[] regions = getRegions(x1, y1, x2, y2, numRegions, spaceSize);
            Text output_record = new Text("r-" + x1 + "-" + y1 + "-" + x2 + "-" + y2);

            // Outputting KV pair <region, original record value> for each KEY
            for (int i = 0; i < regions.length; i++) {
                context.write(regions[i], output_record);
            }
        }
    }

    /**
     * Get a point, return the id (number) of the region
     * @param x1 - x of point
     * @param y1 - y of point
     * @return - int acting as region's 'id'
     */
    public static IntWritable getRegionNum(int x1, int y1, int numRegions, int spaceSize){
        // Getting "coordinate pair" of region in an imaginary grid space
        int regionX = (int) Math.ceil((double) x1 / (spaceSize / Math.sqrt(numRegions)));
        int regionY = (int) Math.ceil((double) y1 / (spaceSize / Math.sqrt(numRegions)));

        // Use those to get the region number
        int region_int = regionX + 5 * (regionY-1);
        IntWritable region = new IntWritable(region_int);

        return region;
    }

    /**
     * Get a point, return the regional coordinates of the region it's in
     * @param x1 - x of point
     * @param y1 - y of point
     * @return - array of length 2 containing regional x (index 0) and regional y (index 1)
     */
    public static int[] getRegionCoords(int x1, int y1, int numRegions, int spaceSize){
        // Getting "coordinate pair" of region in an imaginary grid space
        int[] coords = new int[2];
        coords[0] = (int) Math.ceil((double) x1 / (spaceSize / Math.sqrt(numRegions)));
        coords[1] = (int) Math.ceil((double) y1 / (spaceSize / Math.sqrt(numRegions)));

        return coords;
    }

    /**
     * Get a 2 points, return a list of all region id's (nums) that it overlaps
     * @param x1 - x of point 1
     * @param y1 - y of point 1
     * @param x2 - x of point 2
     * @param y2 - y of point 2
     * @return - list of IntWriteables that will be used as keys
     */
    public static IntWritable[] getRegions(int x1, int y1, int x2, int y2, int numRegions, int spaceSize){
        // Getting the coords of corner regions, finding num regions that R touches
        int[] p1_coords = getRegionCoords(x1, y1, numRegions, spaceSize);
        int[] p2_coords = getRegionCoords(x2, y2, numRegions, spaceSize);
        int num_regions_touched = (1 + Math.abs(p1_coords[0] - p2_coords[0])) * (1 + Math.abs(p1_coords[1] - p2_coords[1]));
        IntWritable[] regions = new IntWritable[num_regions_touched];

        // Adding each of the regions touched to the list of regions
        int numProcessed = 0;
        for (int x = p1_coords[0]; x <= p2_coords[0]; x++){
            for (int y = p1_coords[1]; y <= p2_coords[1]; y++){
                int region_int = x + 5 * (y-1);
                regions[numProcessed] = new IntWritable(region_int);
                numProcessed++;
            }
        }

        // Returning this list
        return regions;
    }

    public static class MapReducer extends Reducer<IntWritable, Text, Text, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("For region: " + key.get());
            // Initializing ArrayLists that need to hold the info
            ArrayList<Integer> point_xs = new ArrayList<Integer>();
            ArrayList<Integer> point_ys = new ArrayList<Integer>();
            ArrayList<Integer> rect_x1s = new ArrayList<Integer>();
            ArrayList<Integer> rect_y1s = new ArrayList<Integer>();
            ArrayList<Integer> rect_x2s = new ArrayList<Integer>();
            ArrayList<Integer> rect_y2s = new ArrayList<Integer>();

            // Value can contain multiple records from both R and P
            for (Text value: values){
                // Processing input line
                String line = value.toString();
                String[] tokens = line.split("-");

                // Value is either from points or rectangles
                if(tokens[0].equals("p")){
                    point_xs.add(Integer.parseInt(tokens[1]));
                    point_ys.add(Integer.parseInt(tokens[2]));
                }
                else if(tokens[0].equals("r")){
                    rect_x1s.add(Integer.parseInt(tokens[1]));
                    rect_y1s.add(Integer.parseInt(tokens[2]));
                    rect_x2s.add(Integer.parseInt(tokens[3]));
                    rect_y2s.add(Integer.parseInt(tokens[4]));
                }
            }

            System.out.println("\t" + rect_x1s.size() + " rectangles and " + point_xs.size() + " points in this region");

            // Need to compare every point in the region to every rectangle in the region
            // This is where we write it to output if they overlap
            int numOverlaps = 0;
            for (int rI = 0; rI < rect_x1s.size(); rI ++){
                for (int pI = 0; pI < point_xs.size(); pI ++){
                    // Comparing point x to rectangle x's
                    if ( (point_xs.get(pI) >= rect_x1s.get(rI)) && (point_xs.get(pI) <= rect_x2s.get(rI)) ){
                        // Comparing point y to rectangle y's
                        if ( (point_ys.get(pI) >= rect_y1s.get(rI)) && (point_ys.get(pI) <= rect_y2s.get(rI)) ){
                            // This is where the output is formatted and written
                            context.write(
                                    new Text("region-" + key.get() + "-rect (" + rect_x1s.get(rI) + "," + rect_y1s.get(rI) + "," + rect_x2s.get(rI) + ","+  + rect_y2s.get(rI) + ")"),
                                    new Text("(" + point_xs.get(pI) + ", " + point_ys.get(pI) + ")"));
                            numOverlaps++;
                        }
                    }
                }
            }
            System.out.println("\t" + numOverlaps + " overlaps in this region");
        }
    }

    public static void start(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SpatialJoinMapReduce");

        System.out.println("Configuration info: \n\tThere are " + numRegions + " regions");
        System.out.println("\tThere are " + (numPoints/numRegions) + " points per region");

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MapReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
