package MRImpl;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.umd.cloud9.io.array.ArrayListWritable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CommonFriendsMapperUsingList extends Mapper<LongWritable, Text, Text, Text>{
    private static final Logger logger = LoggerFactory.getLogger(CommonFriendsMapperUsingList.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.info("Start Mapping...");
        String[] tokens = StringUtils.split(value.toString().trim(), '\t');
        logger.info("user:" + tokens[0] + " - friend:" + tokens[1]);
        Text userID = new Text(tokens[0]);
        Text friendID = new Text(tokens[1]);
        context.write(userID, friendID);
        logger.info("Finished Mapping...");
    }
}
