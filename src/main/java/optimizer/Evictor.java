package optimizer;

import index.IndexesPool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Evictor{
    private final IndexesPool indexPool;

    //look for the highest important data  and replace it with lower importnat
    //optimizer.get

    private boolean fullScanWorking = false;
    private  final BlockingQueue <EngineRotater.SpecificRule> fullScanQueue;

    public Evictor(IndexesPool indexesPool){
        this.indexPool = indexesPool;
        fullScanQueue = new ArrayBlockingQueue<EngineRotater.SpecificRule>(100);

    }

   public void addToFullScan(EngineRotater.SpecificRule rule){
       try {
           if(rule.source.host == EngineRotater.LOCALHOST)  //TODO consider other sources..
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
