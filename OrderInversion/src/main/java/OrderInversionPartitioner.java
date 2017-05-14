package main.java;

/*
* we need the custom partitioner to let all the PairOfWords object be sent to the same reducer
* */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


public class OrderInversionPartitioner implements Partitioner<PairOfWords, IntWritable> {

    @Override
    public void configure(JobConf job) {

    }

    @Override
    public int getPartition(PairOfWords key, IntWritable value, int numPartitions) {
        return 0;
    }
}
