package MapReduce.InMemory;

import MapReduce.TimeSeriesEntry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MovingAverageReducer extends Reducer<Text, TimeSeriesEntry, Text, Text>{

    private static final Logger logger = LoggerFactory.getLogger(MovingAverageReducer.class);
    private int windowSize = 5;
    private double sum = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // get configuration parameter from hadoop
        this.windowSize = context.getConfiguration().getInt("moving.average.window.size", 5);
        logger.info("setup(): key="+windowSize);
    }

    @Override
    protected void reduce(Text key, Iterable<TimeSeriesEntry> values, Context context) throws IOException, InterruptedException {
        // this step is the reason why this solution is in memory
        List<TimeSeriesEntry> sortedTimeSeries = new ArrayList<TimeSeriesEntry>();
        for (TimeSeriesEntry entry : values){
            sortedTimeSeries.add(TimeSeriesEntry.copy(entry));
        }
        Collections.sort(sortedTimeSeries);
        logger.info("reduce(): sortedTimeSeries=" + sortedTimeSeries);

        // calculate prefix sum
        // NOTICE the -1 here
        for (int i=0;i<windowSize-1;i++){
            sum += sortedTimeSeries.get(i).getValue();
        }

        Text outputValue = new Text();

        for (int i=windowSize-1;i<sortedTimeSeries.size();i++){
            logger.info("reduce(): key="+key.toString() + "  row="+i);
            sum +=  sortedTimeSeries.get(i).getValue();
            double movingAverage = sum / windowSize;
            long timestamp = sortedTimeSeries.get(i).getTimestamp();
            outputValue.set(timestamp + "-" + movingAverage);
            context.write(key, outputValue);

            sum -= sortedTimeSeries.get(i-windowSize-1).getValue();
        }

    }
}
