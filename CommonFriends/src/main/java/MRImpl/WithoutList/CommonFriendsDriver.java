package MRImpl.WithoutList;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonFriendsDriver extends Configured implements Tool{

    private static final Logger logger = LoggerFactory.getLogger(CommonFriendsDriver.class);
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("CommonFriendsDriver");

        job.setMapperClass(CommonFriendsMapper.class);
        job.setCombinerClass(CommonFriendsCombiner.class);
        job.setReducerClass(CommonFriendsReducer.class);


        // add jars to distributed cache
//        HadoopUtil.addJarsToDistributedCache(job, "/lib/");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);			// mapper will generate key as Text (the keys are as (person1,person2))
        job.setOutputValueClass(Text.class);		// mapper will generate value as Text (list of friends)

        // args[0] = input directory
        // args[1] = output directory
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        logger.info("run(): status=" + status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: Argument 1: input dir, Argument 2: output dir");
        }

        logger.info("inputDir=" + args[0]);
        logger.info("outputDir=" + args[1]);
        int jobStatus = ToolRunner.run(new CommonFriendsDriver(),args);
        logger.info("jobStatus=" + jobStatus);
        System.exit(jobStatus);
    }

}
