package MapReduce.MRFramework;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
* composite-key: name + timestamp
* natural-key: name
* natural-value: timestamp + value
*
* We need a composite key composed by the name and timestamp so that we can sort by the name along with the timestamp
* implementing the WritableComparable interface for storing on HDFS
* */
public class CompositeKey implements WritableComparable<CompositeKey>{

    private String name;
    private long timestamp;

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public CompositeKey(String name, long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "CompositeKey{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public int compareTo(CompositeKey o) {

        if (this.name.equals(o.getName())){
            return this.timestamp < o.getTimestamp() ? 0 : 1;
        }

        return this.name.compareTo(o.getName()) ;
    }

    public void write(DataOutput out) throws IOException {
        out.writeChars(name);
        out.writeLong(timestamp);
    }

    public void readFields(DataInput in) throws IOException {
        this.name = in.readLine();
        this.timestamp = in.readLong();
    }

}
