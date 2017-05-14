package hadoop;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator(){
       super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2){
        CompositeKey ck1 = (CompositeKey) wc1;
        CompositeKey ck2 = (CompositeKey) wc2;

        int comparison = ck1.getStockSymbo().compareTo(ck2.getStockSymbo());
        if (comparison == 0){
            if (ck1.getTimestamp() == ck2.getTimestamp()){
                return 0;
            }else if(ck1.getTimestamp() < ck2.getTimestamp()){
                return -1;
            }else{
                return 1;
            }
        }else {
            return comparison;
        }
    }
}
