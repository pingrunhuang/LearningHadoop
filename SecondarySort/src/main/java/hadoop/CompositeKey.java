package hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey>{

    private String stockSymbo;
    private long timestamp;

    public CompositeKey(){

    }

    public void set(String stockSymbo, long timestamp){
        this.stockSymbo = stockSymbo;
        this.timestamp = timestamp;
    }

    public String getStockSymbo(){
        return this.stockSymbo;
    }

    public long getTimestamp(){
        return this.timestamp;
    }

    @Override
    public int compareTo(CompositeKey o) {
        if (this.stockSymbo.compareTo(o.stockSymbo) != 0){
            return this.stockSymbo.compareTo(o.stockSymbo);
        }else if (this.timestamp != o.timestamp){
            return this.timestamp < o.timestamp ? -1:1;
        }else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.stockSymbo);
        out.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.stockSymbo = in.readUTF();
        this.timestamp = in.readLong();
    }
}
