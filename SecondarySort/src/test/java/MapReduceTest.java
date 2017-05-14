import mapreduce.DateTemperaturePair;
import mapreduce.SecondarySortingTemperatureMapper;
import mapreduce.SecondarySortingTemperatureReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTest {
    MapDriver<LongWritable, Text, DateTemperaturePair, IntWritable> mapDriver;
    ReduceDriver<DateTemperaturePair, IntWritable, Text, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, DateTemperaturePair, IntWritable, Text, Text> mapReduceDriver;

    @Before
    public void setup(){
        SecondarySortingTemperatureMapper mapper = new SecondarySortingTemperatureMapper();
        SecondarySortingTemperatureReducer reducer = new SecondarySortingTemperatureReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("2000,12,04,10"));
        DateTemperaturePair reduceKey = new DateTemperaturePair(new Text("2000" + "12"), new Text("04"),
                new IntWritable(10));
        mapDriver.withOutput(reduceKey, new IntWritable(10));
        mapDriver.runTest();
    }
    @Test
    public void testReducer() throws IOException {
        DateTemperaturePair dtp1 = new DateTemperaturePair(new Text("2000-11"),new Text("01"),
                new IntWritable(10));
        DateTemperaturePair dtp2 = new DateTemperaturePair(new Text("2001-11"),new Text("01"),
                new IntWritable(5));
        List values = new ArrayList<IntWritable>();
//        ascending
        values.add(new IntWritable(5));
        values.add(new IntWritable(10));
        reduceDriver.withInput(dtp1, values);
        reduceDriver.withInput(dtp2, values);
        reduceDriver.withOutput(new Text("2000-11"),new Text("5,10,"));
        reduceDriver.withOutput(new Text("2001-11"),new Text("5,10,"));
        reduceDriver.runTest();
    }
}
