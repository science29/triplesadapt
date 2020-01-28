package optimizer;

import index.IndexesPool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Evictor{
    private final IndexesPool indexPool;

    //look for the highest important data  and replace it with lower importnat
    //optimizer.get

    private boolean fullScanWorking = false;
    private  final BlockingQueue <Optimiser.SpecificRule> fullScanQueue;

    public Evictor(IndexesPool indexesPool){
        this.indexPool = indexesPool;
        fullScanQueue = new ArrayBlockingQueue<Optimiser.SpecificRule>(100);

    }

   public void addToFullScan(Optimiser.SpecificRule rule){
       try {
           if(rule.source.host == Optimiser.LOCALHOST)  //TODO consider other sources..
               fullScanQueue.put(rule);
       } catch (InterruptedException e) {
           e.printStackTrace();
       }
   }


   private void startFullScan(){
        LocalFullScanWork fullScanWork = new LocalFullScanWork();
        fullScanWork.start();
   }



    public class LocalFullScanWork extends  Thread{


        @Override
        public void run(){
            fullScanWorking = true;


            fullScanWorking = false;



        }

    }


}
