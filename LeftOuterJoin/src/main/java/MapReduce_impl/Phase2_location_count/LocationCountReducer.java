package MapReduce_impl.Phase2_location_count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class LocationCountReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        int count = 0;
        for (Text value : values){
            count++;
        }
        context.write(key, new Text(Integer.toString(count)));
    }
}
