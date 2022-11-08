import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[2]), true);

        Job job = Job.getInstance(conf, "Query 2");
        JobConf jobConf = new JobConf();
        jobConf.setJarByClass(QueryTwo.class);
//        job.setMapperClass(MapCustomer.class);
//        job.setMapperClass(MapTransaction.class);
        job.setCombinerClass(Query2.TokenizerReducer.class);
        job.setReducerClass(Query2.TokenizerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Query2.MapCustomer.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query2.MapTransaction.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    }
}
