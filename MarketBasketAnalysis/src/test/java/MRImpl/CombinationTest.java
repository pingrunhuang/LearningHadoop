package MRImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

public class CombinationTest {
    private static final Logger logger = LoggerFactory.getLogger(CombinationTest.class);
    private List<String> data;
    @BeforeTest
    public void setup(){
        data = new ArrayList<String>();
        data.add("a");
        data.add("b");
        data.add("c");
        data.add("d");
    }

    @Test
    public void testFindSortedTuples(){

        List<List<String>> expected = new ArrayList<List<String>>();
        List<String> combination = new ArrayList<String>();
        String[] test_data = "a,b],[a,c],[a,d],[b,c],[b,d],[c,d".split("\\],\\[");

        for (String str : test_data){
            String[] temp = str.split(",");
            for (String t : temp){
                combination.add(t);
            }
            expected.add(combination);
            combination = new ArrayList<String>();
        }
        List<List<String>> actual = Combination.findSortedTuples(data,2);
        logger.info("Actual output: " + actual.toString());
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testFindSorted3Tuples(){

        List<List<String>> expected = new ArrayList<List<String>>();
        List<String> combination = new ArrayList<String>();
        String[] test_data = new String[]{"a,b,c","a,b,d","a,c,d","b,c,d"};
        for (String str : test_data){
            String[] temp = str.split(",");
            for (String t : temp){
                combination.add(t);
            }
            expected.add(combination);
            combination = new ArrayList<String>();
        }
        List<List<String>> actual = Combination.findSortedTuples(data, 3);
        logger.info("Actual output: " + actual.toString());
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testFindSortedTuplesWith5(){
        // when the required value n is larger then the list's size
        // should return a list with zero size
        List<List<String>> expected = new ArrayList<List<String>>();
        List<List<String>> actual = Combination.findSortedTuples(data, 5);
        logger.info("Actual output: " + actual.toString());
        Assert.assertEquals(actual, expected);
    }
}