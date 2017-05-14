package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapred.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
public class SecondarySortingTemperatureMapper extends Mapper<LongWritable, Text, DateTemperaturePair, IntWritable> {
    private static Logger logger = LoggerFactory.getLogger(SecondarySortingTemperatureMapper.class);
//    define your own mapper
    @Override
    protected void map(LongWritable temp, Text input, Context context)
        throws InterruptedException, IOException{
        logger.info("Starting to run mapper");
        String[] tokens = input.toString().split(",");
        Text yearMonth = new Text(tokens[0] + "-" + tokens[1]);
        Text day = new Text(tokens[2]);
        IntWritable temperature = new IntWritable(Integer.parseInt(tokens[3]));

        DateTemperaturePair reduceKey = new DateTemperaturePair(yearMonth,day,temperature);

        context.write(reduceKey,temperature);
        logger.info("Mapper complete");
    }
}
