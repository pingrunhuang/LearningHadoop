package MapReduce_impl.Phase1_Left_Join;


import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LeftJoinUserMapper extends Mapper<Object, Text, PairOfStrings, PairOfStrings>{

    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();

    @Override
    protected void map(Object key, Text value,
                       Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(),"\t");
        String userID = tokens[0];
        String locationID = tokens[1];

        // adding the "1" at the end to be able to use secondary sort at the reducer phase
        outputKey.set(userID,"1");
        outputValue.set("L", locationID);

        context.write(outputKey, outputValue);
    }

}
