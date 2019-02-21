package index;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import triple.Vertex;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;


public class VertexGraphIndex {

    private final String fileName;
    private DB dbMemory;
    private DB dbFile;
    HTreeMap inMemory;
    HashMap<Long,ArrayList<Vertex>> normalMap = new HashMap<Long,ArrayList<Vertex>>();

    private final String HOME_DIR = "/home/ahmed/";

    public VertexGraphIndex(String fileName){
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
                .hashMap("onDisk"+fileName, Serializer.LONG,Serializer.STRING)
                .create();
        // fast in-memory collection with limited size
        HTreeMap inMemory = dbMemory
                .hashMap("fastMap"+fileName ,Serializer.LONG,Serializer.STRING)
                .expireStoreSize(500 *1024*1024)
                //this registers overflow to `onDisk`
                .expireOverflow(onDisk)
                //good idea is to enable background expiration
                .expireExecutor(Executors.newScheduledThreadPool(2))
                .create();

        return  inMemory;
    }



    public void put(Long key, ArrayList<Vertex> value) {
        normalMap.put(key,value);
    }


    public ArrayList<Vertex> get(Long key){
        return normalMap.get(key);
        //return (ArrayList<Vertex>) fastMap.get(key);
    }

    public boolean containsKey(Object key) {
       return normalMap.containsKey(key);
        //return fastMap.containsKey(key);
    }


    public Set<Map.Entry<Long, ArrayList<Vertex>>> entrySet() {
        return normalMap.entrySet();
       // return fastMap.entrySet();
    }

}
