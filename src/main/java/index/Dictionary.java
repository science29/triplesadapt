package index;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

public class Dictionary{

    private final String fileName;
    private DB dbMemory;
    private DB dbFile;
    HTreeMap inMemory;
    HashMap<String,Long> normalMap = new HashMap<String, Long>();
    HashMap<String,Long> cache = new HashMap<String, Long>();

    private final String HOME_DIR = "/home/ahmed/";


    private boolean cacheEnabled = true;
    public Dictionary(String fileName){
        this.fileName = fileName;
        File file = new File(HOME_DIR + fileName + "db.db");
       /* if (file.exists())
            file.delete();*/
        if(dbFile == null) {
            dbFile = DBMaker
                    .fileDB(file)
                    .fileMmapEnable()
                    .allocateStartSize( 1000 * 1024*1024) // 1GB
                    .allocateIncrement(100 * 1024*1024)
                    .make();
        }
        if(dbMemory == null) {
            dbMemory = DBMaker
                    .memoryDB()
                    .make();
        }
        inMemory = getMap();
    }




    public HTreeMap getMap(){
        // Big map populated with data expired from cache
        HTreeMap onDisk = dbFile
                .hashMap("onDisk"+fileName, Serializer.STRING,Serializer.LONG)
                .createOrOpen();
        // fast in-memory collection with limited size
        HTreeMap inMemory = dbMemory
                .hashMap("inMemory"+fileName ,Serializer.STRING,Serializer.LONG)
                .expireStoreSize(500 *1024*1024)
                //this registers overflow to `onDisk`
                .expireOverflow(onDisk)
                //good idea is to enable background expiration
                .expireExecutor(Executors.newScheduledThreadPool(2))
                .createOrOpen();

        return  onDisk;
    }



    private boolean addToCache(String key, Long value){
        if(cacheEnabled){
            cache.put(key ,value);
            if(cache.size() == 2000000)
                writeCacheToPersist();
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

    public void put(String key, Long value) {
       // normalMap.put(key,value);
        if(!addToCache(key,value))
            inMemory.put(key,value);
    }


    public Long get(String key){
       // return (Long)normalMap.get(key);
       Long val = (Long)inMemory.get(key);
       if(val != null)
           return val;
       return getFromCache(key);
    }


    private Long getFromCache(Object key) {
        if(cacheEnabled)
            return cache.get(key);
        return null;
    }

    public boolean containsKey(Object key) {
        //return normalMap.containsKey(key);
        return inMemory.containsKey(key);
    }


    public Set<Map.Entry<String,Long>> entrySet() {
        //return normalMap.entrySet();
        return inMemory.entrySet();
    }

    public void close() {
        writeCacheToPersist();
        dbFile.commit();
        dbMemory.commit();
        dbFile.close();
        dbMemory.close();

    }
}
