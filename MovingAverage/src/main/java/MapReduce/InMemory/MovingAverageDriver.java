package MapReduce.InMemory;

import MapReduce.TimeSeriesEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MovingAverageDriver extends Configured implements Tool{

    private static final Logger logger = LoggerFactory.getLogger(MovingAverageDriver.class);

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf,args).getRemainingArgs();

        if (args.length != 3){
            logger.error("Usage: SortInMemory_MovingAverageDriver <window_size> <input> <output>");
            System.exit(1);
        }
        logger.debug("window_size: " + args[0]);
        logger.debug("input_dir: " + args[1]);
        logger.debug("output_dir: " + args[2]);

        Job job = Job.getInstance(conf,"Moving.average.in.memory.driver");

        job.setMapperClass(MovingAverageMapper.class);
        job.setReducerClass(MovingAverageReducer.class);

        // define mapper output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TimeSeriesEntry.class);

        // define reducer output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set window size for moving average calculation
        int windowSize = Integer.parseInt(args[0]);
        // notice this is just for test, we can use jar -D for providing arguments
        job.getConfiguration().setInt("moving.average.window.size", windowSize);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new MovingAverageDriver(), args);
        System.exit(result);
    }

}
