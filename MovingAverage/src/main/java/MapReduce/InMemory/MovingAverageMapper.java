package MapReduce.InMemory;


import MapReduce.TimeSeriesEntry;
import Util.DateUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

public class MovingAverageMapper extends Mapper<IntWritable, Text, Text, TimeSeriesEntry>{

    private final static Logger logger = LoggerFactory.getLogger(MovingAverageMapper.class);
    private final Text reduceKey = new Text();
    private final TimeSeriesEntry reduceValue = new TimeSeriesEntry();
    @Override
    // override the map function for moving average
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();

        if (record == null || record.length() == 0){
            return;
        }

        String[] fields = StringUtils.split(record.trim(), ',');
        if (fields.length == 3){
            Date date = DateUtil.getDate(fields[1]);
            reduceKey.set(fields[0]);
            reduceValue.set(date.getTime(),Double.parseDouble(fields[2]));
            logger.info(String.format("Stock name: {}; timestamp: {}; value: {}", fields[0], date.getTime(), fields[2]));
            context.write(reduceKey,reduceValue);
        }else{
            logger.error("Data is not complete!");
        }
    }
}
