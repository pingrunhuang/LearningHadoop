package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SecondarySortDriver extends Configured implements Tool{
    Logger logger = LoggerFactory.getLogger(SecondarySortDriver.class);

    public int run(String[] args) throws Exception {
        logger.info("Starting the secondary sort driver...");
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        //we tell Hadoop to find out the relevant jar by
        // finding out that the class specified as it's parameter to be present as part of that jar
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("mapreduce.SecondarySortDriver");

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // specify the output type
        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SecondarySortingTemperatureMapper.class);
        job.setReducerClass(SecondarySortingTemperatureReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        logger.info("run(): status=" + status);
        return status ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            throw new IllegalArgumentException("Usage: mapreduce.SecondarySortDriver <input-path> <outpu-path>");
        }
        // call the ToolRunner
        int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
        System.exit(returnStatus);
    }
}
