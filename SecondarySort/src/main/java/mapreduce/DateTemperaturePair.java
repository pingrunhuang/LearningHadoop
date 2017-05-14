package mapreduce;/*
* 在Hadoop中，要持久存储定制数据类型，则必须实现Writable接口
* 如果要比较定制数据类型，则还需要实现WritableComparable接口
* 自定义数据类型
* */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateTemperaturePair implements
        Writable, WritableComparable<DateTemperaturePair>{

    private Text yearMonth = new Text();
    private Text day = new Text();
    private IntWritable temperature = new IntWritable();

    public DateTemperaturePair(){

    }

    public DateTemperaturePair(Text yearMonth, Text day, IntWritable temperature){
        this.setYearMonth(yearMonth);
        this.setTemperature(temperature);
        this.setDay(day);
    }

    public Text getYearMonth(){
        return this.yearMonth;
    }
    public Text getDay(){ return this.day; }
    public IntWritable getTemperature(){
        return this.temperature;
    }
    public void setYearMonth(Text yearMonth) { this.yearMonth = yearMonth; }
    public void setDay(Text day) { this.day = day; }
    public void setTemperature(IntWritable temperature) { this.temperature = temperature; }

    /*
    * this comparator will control the key ordering
    * */
    public int compareTo(DateTemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if(compareValue == 0){
            compareValue = temperature.compareTo(pair.getTemperature());
        }
        /*ascending*/
         return compareValue;
        /*descending*/
//        return -1*compareValue;
    }


    public void write(DataOutput dataOutput) throws IOException {
        yearMonth.write(dataOutput);
        day.write(dataOutput);
        temperature.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        yearMonth.readFields(dataInput);
        day.readFields(dataInput);
        temperature.readFields(dataInput);
    }

    public static DateTemperaturePair read(DataInput in) throws IOException{
        DateTemperaturePair pair = new DateTemperaturePair();
        pair.readFields(in);
        return pair;
    }

    @Override
    public boolean equals(Object o){
        if (!(o instanceof DateTemperaturePair)){
            return false;
        }
        DateTemperaturePair t = (DateTemperaturePair)o;
        if(t.getYearMonth().equals(((DateTemperaturePair) o).getYearMonth()) &&
                t.getTemperature().equals(((DateTemperaturePair) o).getTemperature())){
            return true;
        }
        return false;
    }
}
