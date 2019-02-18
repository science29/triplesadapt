package index;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.util.HashMap;
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

    private final String HOME_DIR = "/home/ahmed/";

    public Dictionary(String fileName){
        this.fileName = fileName;
        File file = new File(HOME_DIR + fileName + "db.db");
        if (file.exists())
            file.delete();
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
                .create();
        // fast in-memory collection with limited size
        HTreeMap inMemory = dbMemory
                .hashMap("inMemory"+fileName ,Serializer.STRING,Serializer.LONG)
                .expireStoreSize(500 *1024*1024)
                //this registers overflow to `onDisk`
                .expireOverflow(onDisk)
                //good idea is to enable background expiration
                .expireExecutor(Executors.newScheduledThreadPool(2))
                .create();

        return  inMemory;
    }



    public void put(String key, Long value) {
       // normalMap.put(key,value);
       inMemory.put(key,value);
    }


    public Long get(String key){
       // return (Long)normalMap.get(key);
       return (Long)inMemory.get(key);
    }

    public boolean containsKey(Object key) {
        //return normalMap.containsKey(key);
        return inMemory.containsKey(key);
    }


    public Set<Map.Entry<String,Long>> entrySet() {
        //return normalMap.entrySet();
        return inMemory.entrySet();
    }

}
