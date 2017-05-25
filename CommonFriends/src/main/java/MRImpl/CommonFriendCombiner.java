package MRImpl;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommonFriendCombiner extends Reducer<Text, Text, Text, Text>{
    private static Text outputKey = new Text();
    private static Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        for (Text value : values){
            stringBuilder.append(value + " ");
        }
        for (Text value : values){
            outputKey.set(key + "," + value);
            outputValue.set(stringBuilder.toString());
            context.write(outputKey,outputValue);
        }
    }
}
