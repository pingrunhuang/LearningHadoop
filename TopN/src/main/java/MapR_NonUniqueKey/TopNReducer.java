package MapR_NonUniqueKey;

/*
 *  Reducer's input are local top N from all mappers.
 *  We have a single reducer, which creates the final top N.
* */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text>{

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context
    ) throws IOException, InterruptedException {

    }
}
