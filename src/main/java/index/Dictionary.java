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

    private final String fileName;
    private DB dbMemory;
    private DB dbFile;
    private DB dbFileReverse;
    HTreeMap fastMap;
    HTreeMap reverseFastMap;
    HashMap<String,Long> cache = new HashMap<String, Long>();
    HashMap<Long,String> reverseCache = new HashMap<Long, String>();

    HashMap<String,Long> cache2 = new HashMap<String, Long>();
    HashMap<Long,String> reverseCache2 = new HashMap<Long, String>();

    HashMap<String,Long> mainWriteCache = cache;
    HashMap<Long,String> mainWriteReverseCache = reverseCache;

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
        open();

    }




    public HTreeMap getMap(){
        // Big map populated with data expired from cache
        HTreeMap onDisk = dbFile
                .hashMap("onDisk"+fileName, Serializer.STRING,Serializer.LONG)
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



    private int maxCacheSize = 10000000;
    private int minCacheSize = 500000;
    private double factor = 2;
    private boolean addToCache(String key, Long value){
        if(cacheEnabled){
            mainWriteCache.put(key ,value);
            mainWriteReverseCache.put(value , key);
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
            Long val = (Long) pair.getValue();
            put(key , val);
        }
        cache = new HashMap<String, Long>();
        cacheEnabled = true;
    }

    public void writeCacheToPersistLazy() {

    }

    public void put(String key, Long value) {
       // normalMap.put(key,value);
        if(!addToCache(key,value)) {
            fastMap.put(key, value);
            reverseFastMap.put(value,key);
        }
    }

    public void put(Long value ,String key) {
        //TODO nothing here yet
    }


    public Long get(String key){
       // return (Long)normalMap.get(key);
        Long val = getFromCache(key);
       if(val != null)
           return val;
       return (Long) fastMap.get(key);
    }

    public String get(Long key){
        String val = getFromCache(key);
        if(val != null)
            return val;
        return (String) reverseFastMap.get(key);
    }


    private Long getFromCache(String key) {
        if(!cacheEnabled)
            return null;
        Long val = cache.get(key);
        if(val != null)
            return val;
        return cache2.get(key);
    }

    private String getFromCache(Long key) {
        if(!cacheEnabled)
            return null;
        String val = reverseCache.get(key);
        if(val != null)
            return val;
        return reverseCache2.get(key);
    }



    public boolean containsKey(String key) {
        //return normalMap.containsKey(key);
        if(cacheEnabled && cache.containsKey(key))
            return true;
        return fastMap.containsKey(key);
    }

    public boolean containsKey(Long key) {
        //return normalMap.containsKey(key);
        if(cacheEnabled && reverseCache.containsKey(key))
            return true;
        return reverseFastMap.containsKey(key);
    }



    public Set<Map.Entry<String,Long>> entrySet() {
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




    class Consumer extends Thread /*implements Runnable*/{
        private BlockingQueue<Long> sharedValQueue;
        private  BlockingQueue<String> sharedkeyQueue;
        public boolean stop = false;


        public Consumer() {
            this.sharedValQueue = new LinkedBlockingQueue<Long>(maxCacheSize*2);
            this.sharedkeyQueue =  new LinkedBlockingQueue<String>(maxCacheSize*2);
        }


        public void add(String key , Long val){
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
                    Long val = sharedValQueue.take();
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
