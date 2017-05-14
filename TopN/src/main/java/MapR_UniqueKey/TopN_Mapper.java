package MapR_UniqueKey;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TopN_Mapper extends Mapper<IntWritable, Text, DoubleWritable, Text>{
    private int N = 10;
    private SortedMap<Double, String> top10Cat = new TreeMap<Double, String>();

    // this method will be executed by each reducer before each task
    @Override
    public void setup(Context context){
        // top.n is set by the driver
        Configuration conf = context.getConfiguration();
        N = conf.getInt("top.n", 10);
    }

    // this method will be executed after each task
    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException{

        for(int i = 0; i<top10Cat.size();i++){
            Double keyTemp = (Double) top10Cat.keySet().toArray()[i];
            DoubleWritable key = new DoubleWritable(keyTemp);
            Text value = (Text) top10Cat.values().toArray()[i];
            context.write(key, value);
        }
    }


}
