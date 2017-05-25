package MRImpl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
* The data input format is [userID, friendID]
* The output data format of mapper is [userID, friendID]
* */
public class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
    private static final Logger loger = LoggerFactory.getLogger(CommonFriendsMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        loger.info("Start Mapping...");
        String[] tokens = StringUtils.split(value.toString().trim(), '\t');
        loger.info("user:" + tokens[0] + " - friend:" + tokens[1]);
        Text userID = new Text(tokens[0]);
        Text friendID = new Text(tokens[1]);
        context.write(userID, friendID);
        loger.info("Finished Mapping...");
    }
}
