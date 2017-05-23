package MapReduce.MRFramework;

import java.util.LinkedList;
import java.util.Queue;

/*
* this is a simple implementation of moving average algorithm
* */
public class MovingAverageAlgo {
    private int windowsize;
    private Queue<Double> queue = new LinkedList<Double>();
    private double sum = 0;
    public MovingAverageAlgo(int size) {
        if (size<1){
            throw new IllegalArgumentException("the window size should be positive!");
        }
        this.windowsize = size;
    }

    public void setWindowsize(int windowsize) {
        this.windowsize = windowsize;
    }

    public int getWindowsize() {
        return windowsize;
    }

    public double getMovingAverage(){
        if (this.windowsize < 0) {
            throw new IllegalArgumentException("Due to window size is less then 0, can't calculate the moving average.");
        }
        return sum / windowsize;
    }

    public void addNewNumber(double num){
        sum += num;
        queue.add(num);
        if (queue.size() > this.windowsize){
            sum -= queue.remove();
        }
    }

}
