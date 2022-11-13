import kmeans.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;


public class KMeans {

    private static HashMap<Integer, Point> centroidMap = new HashMap<>();
    private static HashMap<Integer, Point> tempCentroidMap = new HashMap<>();

/*    static class Distance {
        static double getDistance(Point p1, Point p2) throws Exception {
            double[] p1Vector = p1.getVector();
            double[] p2Vector = p2.getVector();
            double sum = 0f;

            for (int i = 0; i < p1Vector.length; i++) {
                sum += Math.pow(p1Vector[i] - p2Vector[i], 2);
            }
            return Math.sqrt(sum);
        }
    }

    static class Point {
        private double x, y;

        Point(int x, int y) {
            this.x = x;
            this.y = y;
        }

        Point(String x, String y) {
            this.x = Double.parseDouble(x);
            this.y = Double.parseDouble(y);
        }

        Point(String point) {
            System.out.println("point: "+ point);
            String[] vals = point.split(",");
            this.x = Double.parseDouble((vals[0]));
            this.y = Double.parseDouble((vals[1]));
        }

        Point addPoint(Point point) {
            this.x += point.x;
            this.y += point.y;
            return this;
        }

        Point divide(double s) {
            this.x = this.x / s;
            this.y = this.y / s;
            return this;
        }

        double[] getVector() {
            return new double[]{x, y};
        }

        @Override
        public String toString() {
            return x + "," + y;
        }
    }*/


    public static class TokenizerMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {


        protected void setup(Context context) throws IOException, InterruptedException {
            // Setup Distributed File Cache
            try {

                URI[] cache = context.getCacheFiles();
                if (cache != null && cache.length > 0) {
                    for (URI file : cache)
                        loadFile(file, context);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }

        private void loadFile(URI file, Context context) {
            try {
                centroidMap = new HashMap<>();
                FileSystem fs = FileSystem.get(file, context.getConfiguration());
                Path getFilePath = new Path(file.toString());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                String value;
                int centroidCounter = 0;
                while ((value = bufferedReader.readLine()) != null) {
                    centroidCounter += 1;
                    centroidMap.put(centroidCounter, new Point(value));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double distance;
            double[] minDistance = {1e3, 1e6};
            Point point = new Point(value.toString());
            try {
                for (Integer centroidId : centroidMap.keySet()) {
                    distance = Distance.getDistance(point, centroidMap.get(centroidId));
                    if (distance < minDistance[1]) {
                        minDistance[1] = distance;
                        minDistance[0] = centroidId;
                    }
                }
                context.write(new DoubleWritable(minDistance[0]), new Text(point.toString()));
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }

    public static class TokenizerCombiner extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                int count = 0;
                Point pointSum = new Point(0, 0);
                for (Text val : values) {
                    count += 1;
                    pointSum.addPoint(new Point(val.toString()));
                }
                context.write(key, new Text(count + "," + pointSum.toString()));
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }

    public static class TokenizerReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                int count = 0;
                Point pointSum = new Point(0, 0);
                for (Text val : values) {
                    String[] vals = val.toString().split(",");
                    pointSum.addPoint(new Point(Integer.parseInt(vals[1]), Integer.parseInt(vals[2])));
                    count += Integer.parseInt(vals[0]);
                }
                pointSum.divide(count);
                context.write(new Text(""), new Text(pointSum.toString()));
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }

    public void debug(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("hadoop.home.dir", "C://hadoop-3.3.4//");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        conf.set("mapreduce.output.textoutputformat.separator", " ");

        for (int i = 0; i < 6; i++) {
            System.out.println("Iteration number " + i);
            Job job = Job.getInstance(conf, "kmeans");
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeans.TokenizerMapper.class);
            job.setCombinerClass(KMeans.TokenizerCombiner.class);
            job.setReducerClass(KMeans.TokenizerReducer.class);
            job.setNumReduceTasks(1);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new Path(args[1]).toUri());

            if (i > 0) {
                job.addCacheFile(new Path(args[2] + "/" + (i - 1) + "/part-r-00000").toUri());
            } else {
                job.addCacheFile(new Path(args[1]).toUri());
            }
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(args[2] + '/' + i + '/'));
            job.waitForCompletion(true);


            if (i > 1) {
                boolean terminateCondition = true;
                for (Integer key : centroidMap.keySet()) {
                    if (!tempCentroidMap.get(key).equals(centroidMap.get(key))) {
                        terminateCondition = false;
                        break;
                    }
                }
                if (terminateCondition) System.exit(0);
            }
            tempCentroidMap.putAll(centroidMap);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C://hadoop-3.3.4//");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        conf.set("mapreduce.output.textoutputformat.separator", " ");

        for (int i = 0; i < 6; i++) {
            System.out.println("Iteration number " + i);
            Job job = Job.getInstance(conf, "kmeans");
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeans.TokenizerMapper.class);
            job.setCombinerClass(KMeans.TokenizerCombiner.class);
            job.setReducerClass(KMeans.TokenizerReducer.class);
            job.setNumReduceTasks(1);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new Path(args[1]).toUri());

            if (i > 0) {
                job.addCacheFile(new Path(args[2] + "/" + (i - 1) + "/part-r-00000").toUri());
            } else {
                job.addCacheFile(new Path(args[1]).toUri());
            }
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(args[2] + '/' + i + '/'));
            job.waitForCompletion(true);

            if (i > 1) {
                boolean end = true;
                for (Integer key : centroidMap.keySet()) {
                    if (!tempCentroidMap.get(key).equals(centroidMap.get(key))) {
                        end = false;
                        break;
                    }
                }
                if (end) System.exit(0);
            }
            tempCentroidMap.putAll(centroidMap);
        }
    }
}
