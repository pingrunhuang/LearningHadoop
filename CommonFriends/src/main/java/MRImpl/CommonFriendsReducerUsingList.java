package MRImpl;

import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonFriendsReducerUsingList extends Reducer<Text, ArrayListOfLongsWritable, Text , Text>{
    @Override
    protected void reduce(Text key, Iterable<ArrayListOfLongsWritable> values, Context context) throws IOException, InterruptedException {

    }
}
