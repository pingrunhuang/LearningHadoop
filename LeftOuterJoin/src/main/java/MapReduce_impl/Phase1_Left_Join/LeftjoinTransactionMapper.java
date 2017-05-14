package MapReduce_impl.Phase1_Left_Join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
import edu.umd.cloud9.io.pair.PairOfStrings;

import java.io.IOException;

/*
* the output of mapper is (product_id, location_id) which can not be decided by the reducer.
* we need to use PairOfStrings class to identify the sorting flag
*
* */

public class LeftjoinTransactionMapper extends Mapper<Object, Text, PairOfStrings, PairOfStrings>{

    private PairOfStrings outputKey = new PairOfStrings();
    private PairOfStrings outputValue = new PairOfStrings();

    @Override
    protected void map(Object object, Text record, Context context)
            throws IOException, InterruptedException{
        String[] tokens = StringUtils.split(record.toString(), '\t');
        String productID = tokens[1];
        String userID = tokens[2];

        // PROBLEMS: the mapper can't tell which is the productID and which is the userID
        // by adding a "2" at the end of the userID, we can guaranty that the productID is always at the end
        outputKey.set(userID, "2");
        outputValue.set("P", productID);

        context.write(outputKey, outputValue);

    }
}
