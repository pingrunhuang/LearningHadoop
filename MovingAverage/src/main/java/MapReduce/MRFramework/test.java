package MapReduce.MRFramework;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class test {
    @Test
    public void testCompositeKeyCompareTo(){
        CompositeKey key1 = new CompositeKey("abc", 12345L);
        CompositeKey key2 = new CompositeKey("abc", 1234L);
        CompositeKey key3 = new CompositeKey("ab", 123L);
        List<CompositeKey> keys = new ArrayList<CompositeKey>();
        keys.add(key2);
        keys.add(key3);
        keys.add(key1);
        Collections.sort(keys);
        for (CompositeKey key : keys){
            System.out.println(key.toString());
        }
    }

}
