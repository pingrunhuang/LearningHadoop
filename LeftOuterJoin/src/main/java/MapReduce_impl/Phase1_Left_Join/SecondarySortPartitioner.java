package MapReduce_impl.Phase1_Left_Join;


import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<PairOfStrings, PairOfStrings>{
    public int getPartition(PairOfStrings pairOfStrings, PairOfStrings pairOfStrings2, int numPartitions) {
        // & Integer.MAX_VALUE to make sure there are no negative value
        return (pairOfStrings.getLeftElement().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
