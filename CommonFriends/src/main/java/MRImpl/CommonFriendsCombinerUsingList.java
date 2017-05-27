package MRImpl;


import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CommonFriendsCombinerUsingList extends Reducer<Text, Text, Text, ArrayListOfLongsWritable>{
    private static final Logger logger = LoggerFactory.getLogger(CommonFriendsCombinerUsingList.class);

    private final Text outputKey = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        ArrayListOfLongsWritable friends = new ArrayListOfLongsWritable();
        for (Text value: values){
            friends.add(Long.parseLong(value.toString()));
        }

        for (Text value : values) {
            outputKey.set(buildSortedKey(key.toString(), value.toString()));
            context.write(outputKey, friends);
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
