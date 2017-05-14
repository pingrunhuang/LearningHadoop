package MapR_NonUniqueKey;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* this mapper take the output from the previous phase's reducer
* */
public class TopNMapper extends Mapper<Text, IntWritable, NullWritable, IntWritable>{
    @Override
    protected void map(Text key, IntWritable value,
                       Context context) throws IOException, InterruptedException {

    }
}
