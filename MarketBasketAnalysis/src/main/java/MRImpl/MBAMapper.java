package MRImpl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
* the input format is: <item1>,<item2>...
* the output format is:
*   <pair1> <1>
*   <pair2> <1>
*   ...
* */

public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private static final int DEFAULT_NUMBER_OF_ITEMS_EACH_PAIR = 2;
    private int numberOfItemInsidePair;
    private static final Text outputKey = new Text();
    private static final IntWritable NUMBER_ONE = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numberOfItemInsidePair = context.getConfiguration().getInt("number.of.items.each.pair",DEFAULT_NUMBER_OF_ITEMS_EACH_PAIR);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> input_data = convertItemsToList(value.toString());
        if (input_data == null || input_data.size() == 0){
            return;
        }
        generateMapperOutput(input_data,numberOfItemInsidePair,context);
    }

    private static List<String> convertItemsToList(String line){
        if (line == null || line.length() == 0) {
            return null;
        }
        String[] items_str = StringUtils.split(line.trim(), ',');
        if (items_str == null || items_str.length == 0){
            return null;
        }
        List<String> result = new ArrayList<String>();
        for (String item : items_str){
            if (item!=null){
                item = item.trim();
                result.add(item);
            }

        }
        return result;
    }

    private static void generateMapperOutput(List<String> items, int num, Context context)
            throws InterruptedException,IOException, IllegalArgumentException {
        List<List<String>> sortedCombination = Combination.findSortedTuples(items, num);
        for (List<String> list : sortedCombination){
            outputKey.set(list.toString());
            context.write(outputKey,NUMBER_ONE);
        }
    }


}
