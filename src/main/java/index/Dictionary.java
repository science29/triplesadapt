package index;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import triple.Triple;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Dictionary{

    private final boolean ONLY_MEMORY = true;
    private final String fileName;
    private DB dbMemory;
    private DB dbFile;
    private DB dbFileReverse;
    HTreeMap fastMap;
    HTreeMap reverseFastMap;
    HashMap<String,Integer> cache = new HashMap<String, Integer>();
    HashMap<Integer,String> reverseCache = new HashMap<Integer, String>();

    //HashMap<String,Integer> cache2 = new HashMap<String, Integer>();
   // HashMap<Integer,String> reverseCache2 = new HashMap<Integer, String>();

    //HashMap<String,Integer> mainWriteCache = cache;
    //HashMap<Integer,String> mainWriteReverseCache = reverseCache;

    private final String HOME_DIR = "/home/ahmed/";
    private boolean cacheEnabled = true;


    public void open() {
        File file = new File(HOME_DIR + fileName + "db.db");
        try {
            if(dbFile == null) {
                dbFile = DBMaker
                        .fileDB(file)
                        .fileMmapEnable()
                        .closeOnJvmShutdown()
                        .allocateStartSize( 1000 * 1024*1024) // 1GB
                        .allocateIncrement(100 * 1024*1024)
                        //.transactionEnable()
                        .make();
            }
            fastMap = getMap();
            File fileRev = new File(HOME_DIR + fileName + "RevDb.db");
            if(dbFileReverse == null) {
                dbFileReverse = DBMaker
                        .fileDB(fileRev)
                        .fileMmapEnable()
                        .closeOnJvmShutdown()
                        .allocateStartSize( 1000 * 1024*1024) // 1GB
                        .allocateIncrement(100 * 1024*1024)
                        //.transactionEnable()
                        .make();
            }

            reverseFastMap = getReverseMap();
        }catch (Exception e){
            System.out.println("exception found .. ");
          e.printStackTrace();
        }

    }


    public Dictionary(String fileName){
        this.fileName = fileName;
       /* if (file.exists())
            file.delete();*/
       if(!ONLY_MEMORY)
           if(fileName != null)
               open();

    }




    private HTreeMap getMap(){
        // Big map populated with data expired from cache
        HTreeMap onDisk = dbFile
                .hashMap("onDisk"+fileName, Serializer.STRING,Serializer.INTEGER)
                .createOrOpen();
        // fast in-memory collection with limited size
        /*HTreeMap fastMap = dbMemory
                .hashMap("fastMap"+fileName ,Serializer.STRING,Serializer.LONG)
                .expireStoreSize(500 *1024*1024)
                //this registers overflow to `onDisk`
                .expireOverflow(onDisk)
                //good idea is to enable background expiration
                .expireExecutor(Executors.newScheduledThreadPool(2))
                .createOrOpen();*/

        return  onDisk;
    }

    private HTreeMap getReverseMap(){
        HTreeMap onDisk = dbFileReverse
                .hashMap("onDiskRev"+fileName, Serializer.LONG, Serializer.STRING)
                .createOrOpen();
        return onDisk;
    }



    private int maxCacheSize = 1000000000;
    private int minCacheSize = 500000;
    private double factor = 2;
    private boolean addToCache(String key, Integer value){
        if(cacheEnabled){
            cache.put(key ,value);
            reverseCache.put(value , key);
            if(cache.size() > maxCacheSize){
                writeCacheToPersist();
                maxCacheSize =(int)(((double)maxCacheSize)/factor);
                factor = factor-0.18;
                if(maxCacheSize < minCacheSize)
                    maxCacheSize = minCacheSize;
            }
            return true;
        }
        return false;
    }

    public void writeCacheToPersist() {
        if(cache == null || !cacheEnabled)
            return;
        System.out.println("writing Dictionary cache ..");
        cacheEnabled = false;
        Iterator it = cache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            String key = (String) pair.getKey();
            Integer val = (Integer) pair.getValue();
            put(key , val);
        }
        cache = new HashMap<String, Integer>();
        cacheEnabled = true;
    }

    public void writeCacheToPersistLazy() {

    }

    public void put(String key, Integer value) {
       // normalMap.put(key,value);
        if(!addToCache(key,value)) {
            fastMap.put(key, value);
            reverseFastMap.put(value,key);
        }
    }

    @Deprecated
    public void put(Integer value ,String key) {
        //TODO nothing here yet
    }




    public Integer get(String key){
        Integer val = getFromCache(key);
       if(val != null) {
           return val;
       }else
            return (Integer) fastMap.get(key);
    }



    public String get(Integer key){
        String val = getFromCache(key);
        if(val != null)
            return val;
        return (String) reverseFastMap.get(key);
    }


    private Integer getFromCache(String key) {
        if(!cacheEnabled)
            return null;
        Integer val = cache.get(key);
        return val;
    }

    private String getFromCache(Integer key) {
        if(!cacheEnabled)
            return null;
        String val = reverseCache.get(key);
        return val;
    }


    public boolean containsKey(String key) {
        //return normalMap.containsKey(key);
        if(cacheEnabled && cache.containsKey(key))
            return true;
        if(ONLY_MEMORY)
            return false;
        return fastMap.containsKey(key);
    }

    public boolean containsKey(Integer key) {
        //return normalMap.containsKey(key);
        if(cacheEnabled && reverseCache.containsKey(key))
            return true;
        if(ONLY_MEMORY)
            return false;
        return reverseFastMap.containsKey(key);
    }



    public Set<Map.Entry<String,Integer>> entrySet() {
        //return normalMap.entrySet();
        return fastMap.entrySet();
    }

    public void close() {
        writeCacheToPersist();
        if(dbFile != null) {
            dbFile.commit();
            dbFile.close();
        }
        if(dbMemory!= null) {
            dbMemory.commit();
            dbMemory.close();
        }

    }

    public void reLoad() {
        Iterator it = cache.entrySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            String key = (String) pair.getKey();
            Integer val = (Integer) pair.getValue();
            if(val == -112234)
                System.out.println("error diction");
            count++;
        }
        System.out.println("loaded "+count+ " item.");
    }


    class Consumer extends Thread /*implements Runnable*/{
        private BlockingQueue<Integer> sharedValQueue;
        private  BlockingQueue<String> sharedkeyQueue;
        public boolean stop = false;


        public Consumer() {
            this.sharedValQueue = new LinkedBlockingQueue<Integer>(maxCacheSize*2);
            this.sharedkeyQueue =  new LinkedBlockingQueue<String>(maxCacheSize*2);
        }


        public void add(String key , Integer val){
            try {
                sharedkeyQueue.put(key);
                sharedValQueue.put(val);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        public void run() {
            while(!stop){
                try {
                    Integer val = sharedValQueue.take();
                    String key = sharedkeyQueue.take();
                    put(key, val);
                    //System.out.println("Consumed: "+ num + ":by thread:"+threadNo);
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
        }
    }

}
