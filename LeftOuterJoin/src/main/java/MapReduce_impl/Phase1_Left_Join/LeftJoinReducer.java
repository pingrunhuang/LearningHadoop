package MapReduce_impl.Phase1_Left_Join;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

    private Logger logger = LoggerFactory.getLogger(LeftJoinReducer.class);
    private Text locationID = new Text("Undefined");
    private Text productID = new Text();

    @Override
    protected void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context)
            throws IOException, InterruptedException{
        // key is userID
        // the input form should be something like below
        //            key                 value
        //         (userID, 1)       ("L", LocationID)
        //         (userID, 2)       ("P", ProductID)
        //         (userID, 2)       ("P", ProductID)
        //          ...                 ...
        // one location can correspond to many product
        Iterator<PairOfStrings> iter = values.iterator();

        if(iter.hasNext()){
            PairOfStrings firstPair = iter.next();

            logger.debug("First Pair is: " + firstPair.toString());

            if (firstPair.getLeftElement().equals("L")){
                locationID.set(firstPair.getRightElement());
            }
        }

        while (iter.hasNext()){
            PairOfStrings productPair = iter.next();
            logger.debug("Followed by: " + productPair.toString());
            productID.set(productPair.getRightElement());
            context.write(productID,locationID);
        }

    }
}
