package QueryStuff;

import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class InterQueryExecuter extends  Thread{
    private final BlockingQueue<WorkElement> sharedWorkQueue ;
    private final int ID;
    private InterExecutersPool interExecutersPool;

    private boolean stop = false;



    public InterQueryExecuter(BlockingQueue<WorkElement> sharedWorkQueue , int ID , InterExecutersPool interExecutersPool){
        this.sharedWorkQueue = sharedWorkQueue;
        this.ID = ID ;
        this.interExecutersPool = interExecutersPool;
    }


    public int getWorkingSize() {
        return sharedWorkQueue.size();
    }

    @Override
    public void run() {
        while(!stop){
            try {
                interExecutersPool.setWorking(false , ID );
                WorkElement workElement = sharedWorkQueue.take();
                if(stop)
                    break;
                interExecutersPool.setWorking(true , ID );
                workElement.triplePattern.startWorkerDeepEvaluationParallel(workElement.list , workElement.from , workElement.to);
                workElement.completeListener.onComplete();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void addWork(TriplePattern2 triplePattern, ArrayList<Triple> list, int from, int to , CompleteListener completeListener) {
        WorkElement workElement = new WorkElement(triplePattern , list ,from , to ,completeListener);
        try {
            sharedWorkQueue.put(workElement);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void stopWorker(){
        stop = true;
        try {
            sharedWorkQueue.put(new WorkElement());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




    private class WorkElement{
        public ArrayList<Triple> list;
        public int from;
        public int to;
        public TriplePattern2 triplePattern;
      //  public TriplePattern2 copyTriplePattern;
        public CompleteListener completeListener;

        public  WorkElement(){
            //dummy constructor to stop the thread..
        }
        public WorkElement(TriplePattern2 triplePattern, ArrayList<Triple> list, int from, int to , CompleteListener completeListener) {
            this.list = list;
            this.triplePattern = triplePattern ;
            this.from = from;
            this.to = to;
            this.completeListener = completeListener;
           // this.copyTriplePattern = TriplePattern2.getThreadReadyCopy(triplePattern);
        }
    }

    public interface CompleteListener{
        void onComplete();
    }
}
