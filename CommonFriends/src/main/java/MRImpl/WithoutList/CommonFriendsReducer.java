package MRImpl;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text>{

    private static final Logger logger = LoggerFactory.getLogger(CommonFriendsReducer.class);
    Text outputValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text list1 = values.iterator().next();
        Text list2 = values.iterator().next();
        if (list1 == null || list1.getLength() == 0 || list2 == null || list2.getLength() == 0){
            logger.error("Reduce phase should take a input data with 2 list of friends");
            logger.info("list1:" + list1.toString() + ";list2:" + list2.toString());
            System.exit(1);
        }

        String[] friends1 = StringUtils.split(list1.toString().trim(), ' ');
        String[] friends2 = StringUtils.split(list2.toString().trim(), ' ');
        List<String> intersect = new ArrayList<String>();
        for (int i=0;i<friends1.length;i++){
            for (int j=0;j<friends2.length;j++){
                if (friends1[i].equals(friends2[j])){
                    logger.info("Common friend:" + friends1[i]);
                    intersect.add(friends1[i]);
                }
            }
        }
        outputValue.set(intersect.toString());
        context.write(key,outputValue);
        logger.info("Finished Reduce phase");
    }
}
