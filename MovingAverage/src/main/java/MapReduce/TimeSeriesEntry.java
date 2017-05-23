package MapReduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeSeriesEntry implements Writable, Comparable<TimeSeriesEntry>{
    // Writable: to store the entry on hadoop consistently
    // Comparable: sort the entry so that it is ordered in the output

    private long timestamp;
    private double value;

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    public TimeSeriesEntry(long timestamp, double value){
        this.timestamp = timestamp;
        this.value = value;
    }

    public static TimeSeriesEntry copy(TimeSeriesEntry entry){
        return new TimeSeriesEntry(entry.timestamp, entry.value);
    }

    public TimeSeriesEntry(){

    }

    @Override
    public String toString() {
        return "TimeSeriesEntry{" +
                "timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }

    public void set(long timestamp, double value){
        setTimestamp(timestamp);
        setValue(value);
    }

    public int compareTo(TimeSeriesEntry o) {
        return this.timestamp > o.timestamp ? 1 : (this.timestamp == o.timestamp ? 0 : -1);
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(this.timestamp);
        out.writeDouble(this.value);
    }

    public void readFields(DataInput in) throws IOException {
        this.timestamp = in.readLong();
        this.value = in.readDouble();
    }
}
