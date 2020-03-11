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
    HashMap<Integer,Integer> cache = new HashMap<>(); // the key is the hash of the char arr of the string
    HashMap<Integer,char[]> reverseCache = new HashMap<>();
    private HashMap<String, Integer> collissionMap = new HashMap<>();
    private HashMap< Integer , String> reverseCollissionMap = new HashMap<>();

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
            char [] chars = key.toCharArray();
            Integer keyInt = getDictionaryIntKey(chars);
            cache.put(keyInt ,value);
            reverseCache.put(value , chars);
            if(ONLY_MEMORY)
                return true;
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

    private Integer getDictionaryIntKey(char [] chars ) {
        //char [] chars = key.toCharArray();
        int r = chars.length*10000;
        for(int i = 0 ; i < chars.length ; i++){
            int n = chars[i];
            r = r+n*i;
        }
        System.out.println(r);
        return r;
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
        cache = new HashMap<>();
        cacheEnabled = true;
    }

    public void writeCacheToPersistLazy() {

    }

    public void put(String key, Integer value) {
       // normalMap.put(key,value);
        if(collissionMap.containsKey(key)){
            collissionMap.put(key,value);
            this.reverseCollissionMap.put(value,key);
            return;
        }
        if(!addToCache(key,value)) {
            fastMap.put(key.toCharArray(), value);
            reverseFastMap.put(value,key);
        }
    }

    @Deprecated
    public void put(Integer value ,String key) {
        //TODO nothing here yet
    }




    public Integer get(String key){
        Integer val = getFromCache(key);
       if(ONLY_MEMORY || val != null) {
           return val;
       }else
            return (Integer) fastMap.get(key);
    }


    public Integer get(char [] key){
        Integer val = getFromCache(key);
        if(val != null) {
            return val;
        }else
            return (Integer) fastMap.get(key);
    }


    public char[] get(Integer key){
        char[] val = getFromCache(key);
        if(val != null)
            return val;
        return (char[]) reverseFastMap.get(key); //TODO fix wrong casting !
    }


    private Integer getFromCache(String key) {
        Integer val = collissionMap.get(key);
        if(val != null)
            return val;
        if(!cacheEnabled)
            return null;
        int intKey = getDictionaryIntKey(key.toCharArray());
        val = cache.get(intKey);//TODO performance issue?
        return val;
    }

    private Integer getFromCache(char[] key) {
        Integer val = collissionMap.get(new String(key));
        if(val != null)
            return val;
        if(!cacheEnabled)
            return null;
        int intKey = getDictionaryIntKey(key);
        val = cache.get(intKey);
        return val;
    }

    private char[] getFromCache(Integer key) {
        String strVal = reverseCollissionMap.get(key);
        if(strVal != null)
            return strVal.toCharArray();
        if(!cacheEnabled)
            return null;
        char [] val = reverseCache.get(key);
        return val;
    }



    public boolean containsKey(String key, boolean building) {
        boolean res = containsKey(key);
        if(!building){
            return res;
        }
        if(res){
            Integer val = get(key);
            char[] val2 = get(val);
            char[] keyArr = key.toCharArray();
            //int keyInt = getDictionaryIntKey(keyArr);
            for(int j =0 ; j < val2.length ;j++){
                if(val2[j] != keyArr[j]){
                    String val2Str = new String(val2);
                    collissionMap.put(key,-1);
                    return false;
                    //collissionMap.put(key,)
                   // System.err.println("error detected in Dictionary building vals:"+key+" , "+val+ " , " + val2Str+".. exiting");
                   // System.exit(0);
                }
            }
        }
        return  res;
    }

    public boolean containsKey(String key) {
        //return normalMap.containsKey(key);
        int keyInt = getDictionaryIntKey(key.toCharArray());
        if(cacheEnabled && cache.containsKey(keyInt))
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

    public String getString(int key) {
        return new String(get(key));
    }

    public Iterator<Map.Entry<Integer, char[]>> getIterator() {
        return this.reverseCache.entrySet().iterator();
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
