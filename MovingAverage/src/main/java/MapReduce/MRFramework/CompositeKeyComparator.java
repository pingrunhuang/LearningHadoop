package MapReduce.MRFramework;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * this is where the composite key get sorted
 */
public class CompositeKeyComparator extends WritableComparator{
    protected CompositeKeyComparator() {
        super(CompositeKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CompositeKey key1 = (CompositeKey) a;
        CompositeKey key2 = (CompositeKey) b;

        return key1.compareTo(key2);
    }

}
