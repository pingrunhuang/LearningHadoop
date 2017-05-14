package spark;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;


public class TupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {

    public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
        return o2._2.compareTo(o1._2);
    }

}
