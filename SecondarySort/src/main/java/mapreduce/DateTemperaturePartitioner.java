package mapreduce;

/**
 * this partitioner will assign the output from the mapper to a specified reducer
 */
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text>{

    @Override
    public int getPartition(DateTemperaturePair pair, Text text, int numberOfPartitions) {
        // make sure the number of partition is not negative
        // define how to assign the tasks to different reducer, plays the role by setting criteria
        return Math.abs(pair.getYearMonth().hashCode() % numberOfPartitions);
    }

}
