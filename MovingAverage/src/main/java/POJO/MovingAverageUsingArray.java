package POJO;

public class MovingAverageUsingArray {
    private final int period;
    private double[] queue;
    private double sum = 0;
    private int pointer = 0;
    private int size = 0;// size current size from 0 to pointer
    public MovingAverageUsingArray(int period){
        if (period < 1){
            throw new IllegalArgumentException("Period should be larger then 0");
        }
        this.period = period;
        queue = new double[period];
    }

    public void addElement(double value){
        sum += value;
        if (size < period){
            queue[pointer++] = value;
            size++;
        }else{
            pointer = pointer % period;
            sum -= queue[pointer];
            queue[pointer] = value;
        }
    }

    public double getMovingAverage(){
        if (size == 0){
            throw new IllegalArgumentException("Average undefined");
        }
        return sum / size;
    }

}
