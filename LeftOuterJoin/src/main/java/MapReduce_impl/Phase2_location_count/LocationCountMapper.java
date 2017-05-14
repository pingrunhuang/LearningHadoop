package MapReduce_impl.Phase2_location_count;

/*
* This is the second phase.
* The input data is (product_id, location_id) with duplicated location_id
* since different product might be delivered to the same location
* The target of this step is to count the number of product sent to the same location
* */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LocationCountMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] input = StringUtils.split(value.toString(), "\t");
        String product_id = input[0];
        String location_id = input[1];

        context.write(new Text(product_id), new Text(location_id));
    }
}
