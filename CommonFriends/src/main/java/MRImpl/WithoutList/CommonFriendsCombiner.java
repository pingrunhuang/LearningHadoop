package MRImpl;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
            outputKey.set(buildSortedKey(key.toString(),value.toString()));
            outputValue.set(stringBuilder.toString());
            context.write(outputKey,outputValue);
        }
    }
    static String buildSortedKey(String person, String friend) {
        long p = Long.parseLong(person);
        long f = Long.parseLong(friend);
        if (p < f) {
            return person + "," + friend;
        } else {
            return friend + "," + person;
        }
    }
}
