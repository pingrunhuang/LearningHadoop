package mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * this comparator will control which key will be assign to the Reducer.reduce() method
 */
public class DateTemperatureGroupingComparator extends WritableComparator{
    public DateTemperatureGroupingComparator(){
        super(DateTemperaturePair.class, true);
    }
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2){
        DateTemperaturePair pair1 = (DateTemperaturePair) wc1;
        DateTemperaturePair pair2 = (DateTemperaturePair) wc2;
        return pair1.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
