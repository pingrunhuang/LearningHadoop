package MapReduce.MRFramework;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class is used for grouping by natural key in the first phase
 */
public class NaturalKeyGroupingComparator extends WritableComparator{

    public NaturalKeyGroupingComparator() {
        super(CompositeKey.class);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey key1 = (CompositeKey) a;
        CompositeKey key2 = (CompositeKey) b;
        return key1.getName().compareTo(key2.getName());
    }
}
