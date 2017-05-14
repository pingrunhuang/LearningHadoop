package MapReduce_impl.Phase1_Left_Join;


import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LeftJoinDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(LeftJoinDriver.class);

    public int run(String[] args) throws Exception {
        Path transaction_table_path = new Path(args[0]);
        Path user_table_path = new Path(args[1]);
        Path output_path = new Path(args[0]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LeftJoinDriver.class);
        job.setJobName("Phase-1: left outer join");

        // how the mapper generated keys will be partitioned
        job.setPartitionerClass(SecondarySortPartitioner.class);

        // how the natural keys(from mapper) will be grouped
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);

        // how PairOfStrings will be sorted
        job.setSortComparatorClass(PairOfStrings.Comparator.class);

        job.setReducerClass(LeftJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // how to define multiple mappers: one for users and one for transactions
        MultipleInputs.addInputPath(job, transaction_table_path,
                TextInputFormat.class, LeftjoinTransactionMapper.class);
        MultipleInputs.addInputPath(job, user_table_path, TextInputFormat.class, LeftJoinUserMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);
        FileOutputFormat.setOutputPath(job, output_path);

        boolean status = job.waitForCompletion(true);

        logger.info("Run() result : " + status);
        return status ? 1 : 0;
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3){
            throw new IOException("Require 3 arguments! Current number of inputs is " + args.length);
        }

        int status = ToolRunner.run(new LeftJoinDriver(), args);
        System.exit(status);
    }
}
