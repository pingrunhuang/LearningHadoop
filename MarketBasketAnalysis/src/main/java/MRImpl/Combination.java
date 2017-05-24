package MRImpl;

/*
* this class is going to find the sorted tuple of combination
* if data = {a,b,c,d}
* findCollections(data, 2) will return
* {[a,b],[a,c],[a,d],[b,c],[b,d],[c,d]}
* findCollections(data, 3) will return
* {[a,b,c],[a,b,d],[a,c,d],[b,c,d]}
*
*
* This implementation need to be think of carefully since it is how we achieve list and combine mathematical problems
*
* */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Combination {

    private static final Logger logger = LoggerFactory.getLogger(Combination.class);
    // how to declare a generic return type, first declare T type then declare the concrete returning type
    public static <T extends Comparable<? super T >> List<List<T>> findSortedTuples(Collection<T> data, int n)
            throws IllegalArgumentException{
        List<List<T>> result = new ArrayList<List<T>>();

        if (n == 0){
            result.add(new ArrayList<T>());
            return result;
        }

        // get the list of list recursively
        List<List<T>> combinations = findSortedTuples(data, n-1);

        for (List<T> combination : combinations){
            for (T element : data){
                // first check if the existing combination contains the element
                if (!combination.contains(element)){
                    // this list is different from the combination list
                    List<T> list = new ArrayList<T>();
                    list.addAll(combination);
                    if (!list.contains(element)){
                        list.add(element);
                        // sort the list to prevent situation like (a,b) and (b,a) be inserted
                        Collections.sort(list);
                        if (!result.contains(list)){
                            result.add(list);
                        }
                    }
                }
            }
        }

        return result;
    }

}
