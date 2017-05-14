package MapR_NonUniqueKey;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
 * Reducer's input is: (K, List<Integer>)
 *
 * Aggregate on the list of values and create a single (K,V),
 * where V is the sum of all values passed in the list.
 *
 * This class, AggregateByKeyReducer, accepts (K, [2, 3]) and
 * emits (K, 5).
**/
public class AggregateByKeyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    private static Logger logger = LoggerFactory.getLogger(AggregateByKeyReducer.class);
    private IntWritable sum = new IntWritable();
    private Text url = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        url.set(key);
        int result = 0;
        while(values.iterator().hasNext()){
            result = result + values.iterator().next().get();
        }
        sum.set(result);
        context.write(url,sum);
    }
}
