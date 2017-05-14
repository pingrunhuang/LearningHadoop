package main.java;

/*
* This class is going to represent the key of the output of mapper
* eg. (w[i],w[j])
* */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairOfWords implements WritableComparable<PairOfWords> {

    private String left;
    private String right;

    public PairOfWords(String l, String r){
        left = l;
        right = r;
    }
    public void setLeft(String left){
        this.left = left;
    }
    public void setRight(String right){
        this.right = right;
    }
    public String getLeft(){
        return this.left;
    }
    public String getRight(){
        return this.right;
    }

    // compare two pairOfWords according to the sequence from left to right
    @Override
    public int compareTo(PairOfWords o) {
        String l = o.getLeft();
        String r = o.getRight();
        if (left.equals(l)){
            return right.compareTo(r);
        }
        return left.compareTo(l);
    }

    @Override
    public boolean equals(Object o){
        if (!(o instanceof PairOfWords)){
            return false;
        }

        if (((PairOfWords) o).getLeft().equals(left) && ((PairOfWords) o).getRight().equals(right)){
            return true;
        }
        return false;
    }

    @Override
    public int hashCode(){
        return left.hashCode() + right.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, left);
        Text.writeString(out, right);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        left = Text.readString(in);
        right = Text.readString(in);
    }
}
