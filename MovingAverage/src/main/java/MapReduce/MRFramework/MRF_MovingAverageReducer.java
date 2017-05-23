package MapReduce.MRFramework;


import MapReduce.TimeSeriesEntry;
import Util.DateUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class MRF_MovingAverageReducer extends Reducer<CompositeKey, TimeSeriesEntry, Text, Text>{
    protected static Logger logger = LoggerFactory.getLogger(MRF_MovingAverageReducer.class);

    private static int windowSize = 5;
    private static double sum = 0;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.windowSize = context.getConfiguration().getInt("moving.average.window.size", 5);
        logger.info("setup(): key="+windowSize);
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesEntry> values, Context context) throws IOException, InterruptedException {
        Text outputKey = new Text();
        Text outputValue = new Text();

        MovingAverageAlgo movingAverageAlgo = new MovingAverageAlgo(windowSize);
        while (values.iterator().hasNext()){
            TimeSeriesEntry data = values.iterator().next();
            movingAverageAlgo.addNewNumber(data.getValue());
            Double v = movingAverageAlgo.getMovingAverage();
            String timeStamp = DateUtil.getDateAsString(data.getTimestamp());
            outputKey.set(key.getName());
            outputValue.set(timeStamp + "," + v);
        }
    }
}
