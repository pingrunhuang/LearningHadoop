package MapR_NonUniqueKey;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateByKeyDriver extends Configured implements Tool{
    private static Logger logger = LoggerFactory.getLogger(AggregateByKeyDriver.class);

    public int run(String[] args) throws Exception {
        //1. configure driver
        Configuration conf = getConf();

        //2. get job
        Job job = Job.getInstance(conf);

        //3. set job name and jar name
        job.setJarByClass(AggregateByKeyDriver.class);
        job.setJobName("MapR_NonUniqueKey.AggregateByKeyDriver");

        //4. input and output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5. specify output type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6. set the mapper reducer and combiner
        job.setMapperClass(AggregateByKeyMapper.class);
        job.setReducerClass(AggregateByKeyReducer.class);
        job.setCombinerClass(AggregateByKeyReducer.class);

        //7. execute
        boolean status = job.waitForCompletion(true);
        logger.info("run(): status=" + status);
        return status ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 2){
            logger.error("\"usage AggregateByKeyDriver <input> <output>\"");
            System.exit(1);
        }
        int returnStatus = ToolRunner.run(new AggregateByKeyDriver(), args);
        System.exit(returnStatus);
    }
}
