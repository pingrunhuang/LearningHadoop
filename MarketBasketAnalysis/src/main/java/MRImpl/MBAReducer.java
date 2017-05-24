package MRImpl;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
* This reducer is actually going to calculate the support value
* */
public class MBAReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int occurrence = 0;
        for (IntWritable value : values){
            occurrence += value.get();
        }
        context.write(key, new IntWritable(occurrence));
    }
}
