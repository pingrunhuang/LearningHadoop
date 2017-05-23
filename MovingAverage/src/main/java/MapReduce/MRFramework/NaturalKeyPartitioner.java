package MapReduce.MRFramework;

import MapReduce.TimeSeriesEntry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * we need a customized partitioner to assign data entry with the same name instead of same composite key to the same reducer
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, TimeSeriesEntry>{
    public int getPartition(CompositeKey compositeKey, TimeSeriesEntry entry, int numPartitions) {
        return Math.abs((int)hash(compositeKey.getName()) % numPartitions);
    }

    // the hash function is used for determine which reducer this data entry will be assigned to
    // data with the same hash value will be sent to the same reducer
    static long hash(String str){
        long h = 1125899906842597L;
        for (char c : str.toCharArray()){
            h = 31*h + c;
        }
        return h;
    }
}
