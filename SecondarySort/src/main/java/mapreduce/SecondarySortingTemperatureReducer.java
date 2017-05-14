package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SecondarySortingTemperatureReducer extends Reducer<DateTemperaturePair, IntWritable, Text, Text> {
    private static Logger logger = LoggerFactory.getLogger(SecondarySortingTemperatureReducer.class);
    @Override
    protected void reduce(DateTemperaturePair dateTemperaturePair, Iterable<IntWritable> temperature, Context context)
            throws IOException, InterruptedException {
            StringBuilder outputBuilder = new StringBuilder();
            for(IntWritable t : temperature){
                outputBuilder.append(t.toString());
                outputBuilder.append(",");
            }
            Text key =  dateTemperaturePair.getYearMonth();
            logger.info("key: " + key.toString());
            Text value = new Text(outputBuilder.toString());
            context.write(key, value);
    }

}
