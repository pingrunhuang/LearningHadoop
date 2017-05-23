package POJO;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

public class MovingAverageUsingQueue {
    private final int period;
    private double sum;
    private Queue<Double> queue = new LinkedList<Double>();
    public MovingAverageUsingQueue(int p){
        if (p<1){
            throw new IllegalArgumentException("Parameter should > 0");
        }
        this.period = p;
    }

    public void addElement(double value){
        sum += value;
        queue.add(value);
        if (queue.size() > period){
            sum -= queue.remove();
        }
    }

    public double getMovingAverage(){
        if (queue.isEmpty()){
            throw new IllegalArgumentException("the queue should not be empty");
        }
        return sum / queue.size();
    }
}
