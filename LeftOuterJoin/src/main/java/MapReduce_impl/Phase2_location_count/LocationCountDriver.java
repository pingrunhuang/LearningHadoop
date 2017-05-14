package MapReduce_impl.Phase2_location_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationCountDriver extends Configured implements Tool{
    private static Logger logger = LoggerFactory.getLogger(LocationCountDriver.class);

    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        Path input_path = new Path(input);
        Path output_path = new Path(output);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LocationCountDriver.class);
        job.setJobName("Phase-2: count by the number of location");

        job.setMapperClass(LocationCountMapper.class);
        job.setReducerClass(LocationCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job,input_path);
        FileOutputFormat.setOutputPath(job,output_path);

        boolean status = job.waitForCompletion(true);

        return status ? 1 : 0;
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2){
            logger.error(String.format("Input { } arguments. 2 arguments required!", args.length));
            System.exit(-1);
        }

        int status = ToolRunner.run(new LocationCountDriver(), args);
        System.exit(status);
    }
}
