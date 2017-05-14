package MapReduce_impl.Phase1_Left_Join;


import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SecondarySortGroupComparator implements RawComparator<PairOfStrings>{

    private Logger logger = LoggerFactory.getLogger(SecondarySortGroupComparator.class);
    private DataInputBuffer buffer = new DataInputBuffer();
    private PairOfStrings key1 = new PairOfStrings();
    private PairOfStrings key2 = new PairOfStrings();

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try{
            buffer.reset(b1,s1,l1);
            key1.readFields(buffer);
            buffer.reset(b2,s2,l2);
            key2.readFields(buffer);
            return compare(key1,key2);
        }catch (IOException e){
            logger.error(e.toString());
            return -1;
        }
    }

    public int compare(PairOfStrings o1, PairOfStrings o2) {

//        return o1.getRightElement().compareTo(o2.getRightElement());
        return o1.getLeftElement().compareTo(o2.getLeftElement());
    }

    public boolean equals(Object obj) {
        return false;
    }
}
