package MapReduce.MRFramework;

import MapReduce.TimeSeriesEntry;
import Util.DateUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

public class MRF_MovingAverageMapper extends Mapper<LongWritable, Text, CompositeKey, TimeSeriesEntry>{
    protected static Logger logger = LoggerFactory.getLogger(MRF_MovingAverageMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String input_entry = value.toString();
        if (input_entry == null || input_entry.length() == 0){
            logger.error(String.format("Entry {} is illegal!", key));
            System.exit(0);
        }
        String[] tokens = StringUtils.split(input_entry, ',');
        if (tokens.length != 3){
            logger.error(String.format("Entry {} must have 3 fields!", key));
            System.exit(0);
        }

        String name = tokens[0];
        Date date = DateUtil.getDate(tokens[1]);
        if (date == null){
            return;
        }
        String val = tokens[2];
        Long timeStamp = date.getTime();

        CompositeKey output_key = new CompositeKey(name, timeStamp);
        TimeSeriesEntry output_value = new TimeSeriesEntry(timeStamp, Double.parseDouble(val));
        context.write(output_key,output_value);
        logger.info("Map phase finished.");
    }
}
