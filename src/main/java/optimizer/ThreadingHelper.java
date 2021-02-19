package optimizer;

public class ThreadingHelper {



    public static int getOptimalQueryThread(double currenQueueSize , double noOfAvaiThreads) {
        if(currenQueueSize >= noOfAvaiThreads)
            return 1;
        if(currenQueueSize == 0)
            return (int)noOfAvaiThreads;
        int cnt = (int)Math.round(noOfAvaiThreads / currenQueueSize);
        return cnt;
    }
}
