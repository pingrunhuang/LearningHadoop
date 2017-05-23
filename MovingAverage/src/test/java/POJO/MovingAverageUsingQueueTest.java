package POJO;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MovingAverageUsingQueueTest {
    private static Logger logger = LoggerFactory.getLogger(MovingAverageUsingQueue.class);

    @Test
    public void testGetMovingAverage(){
        double[] testData = {10, 18, 20, 30, 24, 33, 27};
        int periods[] = {3, 4};
        for (int period : periods){
            MovingAverageUsingQueue queue = new MovingAverageUsingQueue(period);
            logger.info("The period:" + period);
            for (double data : testData){
                queue.addElement(data);
                logger.info("Next number = "+ data + "; moving average = " + queue.getMovingAverage());
            }
            logger.info("---");
        }
    }

}