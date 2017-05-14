package MapR_NonUniqueKey;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class AggregateByKeyMapper extends Mapper<Object, Text, Text, IntWritable>{
    private static Logger logger = LoggerFactory.getLogger(AggregateByKeyMapper.class);

    @Override
    protected void map(Object key, Text value,
                       Context context) throws IOException, InterruptedException {
        logger.info("Starting to map the 1st phase from (K,V1)...(K,Vm) to (K,V)");
        String input = value.toString();
        String[] tokens = input.split(",");
        Text outputKey = new Text(tokens[0]);
        IntWritable outputValue = new IntWritable(Integer.parseInt(tokens[1]));

        context.write(outputKey, outputValue);

    }
}
