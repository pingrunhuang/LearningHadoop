package MapReduce.MRFramework;


import MapReduce.TimeSeriesEntry;
import Util.HadoopUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRF_MovingAverageDriver extends Configured implements Tool {
    private static Logger logger = LoggerFactory.getLogger(MRF_MovingAverageDriver.class);
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("MRF_MovingAverage_Implementation");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            logger.error("Usage: SortByMRF_MovingAverageDriver <window_size> <input> <output>");
            System.exit(1);
        }

        // add jars to distributed cache
        HadoopUtil.addJarsToDistributedCache(job, "/lib/");

        // set mapper/reducer
        job.setMapperClass(MRF_MovingAverageMapper.class);
        job.setReducerClass(MRF_MovingAverageReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(TimeSeriesEntry.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set window size for moving average calculation
        int windowSize = Integer.parseInt(otherArgs[0]);
        job.getConfiguration().getInt("moving.average.window.size", windowSize);

        // define I/O
        FileInputFormat.setInputPaths(job,new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setCompressMapOutput(true);


        // the following 3 setting are needed for "secondary sorting"
        // Partitioner decides which mapper output goes to which reducer
        // based on mapper output key. In general, different key is in
        // different group (Iterator at the reducer side). But sometimes,
        // we want different key in the same group. This is the time for
        // Output Value Grouping Comparator, which is used to group mapper
        // output (similar to group by condition in SQL).  The Output Key
        // Comparator is used during sort stage for the mapper output key.
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        return status ? 1 : 0;
    }

}
