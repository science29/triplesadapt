package index;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import triple.Triple;
import util.FileHashMap;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;


//TODO solve the  creation of the filehashmap
public class MyHashMap<K, V> extends HashMap<K, V> implements Serializable {

    private static final String ELEME_SEPERATOR = "algheziTr7";
    private static final String TRIPLES_SEPERATOR =  "algheziTr8";
    private String fileName;
    ConcurrentMap<K, V> fastFileMap;
    private HTreeMap<K, String> fastFileMapString;
    DB dbFile;
    private FileHashMap<K, V> fileHashMap;
    private FileHashMap<K, String> backupFileHashMap;

    private HashMap<K, V> hashMap;
    private double elemSize = 0;

    private double maxAllowedRamSize ;
    private final long fac = 16;
    private final long MAX_SIZE_GB =  fac*1000000000;
    private long n = 1;
    private final long GB =  n*1000000000;

    public final IndexType indexType ;

    public static final int [] noKeyType = {0,0,0};

    public boolean onlyDiskGet = false;
    public boolean diskMemPut = false;

    private final String HOME_DIR = "/home/ahmed/";
    private DB dbMemory;


    //   private int avgElemSize = 1 ;
    public MyHashMap(String fileName){
        super();
        try {
            fileHashMap = new FileHashMap(HOME_DIR+fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashMap = new HashMap();
        this.fileName = fileName;
        this.indexType = new IndexType();
        maxAllowedRamSize = MAX_SIZE_GB;
    }

    public MyHashMap(String fileName,double memSizeGB){
        super();
        try {
            fileHashMap = new FileHashMap(HOME_DIR+fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashMap = new HashMap();
        this.fileName = fileName;
        this.indexType = new IndexType();
        this.maxAllowedRamSize = GB * memSizeGB;
    }

    public MyHashMap(String fileName, IndexType indexType) {
        super();
        try {
            fileHashMap = new FileHashMap(HOME_DIR+fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashMap = new HashMap();
        this.fileName = fileName;
        this.indexType = indexType;
        maxAllowedRamSize = MAX_SIZE_GB;
    }


    private void setSize() {
        try {
            if (fastFileMap == null || fastFileMap.size() != 40000 || elemSize != 0 )
                return;

            FileWriter fileWriter = new FileWriter(HOME_DIR+"temp_shjf");
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            int cnt = 0;
            Iterator it = hashMap.entrySet().iterator();
            while (it.hasNext()) {
                if(cnt > 40000)
                    break;
                Map.Entry pair = (Map.Entry)it.next();
                String str = pair.getKey() + " = " + pair.getValue();
                fileWriter.write(str);
                cnt++;
            }
            try {
                bufferedWriter.close();
                fileWriter.close();
            } catch (IOException ex) {

                ex.printStackTrace();

            }
            File file = new File(HOME_DIR+"temp_shjf");
            if(file.length() < 100000000)
                file.length();
            elemSize = (int)file.length()/40000;
                /*
                System.out.println("Index Size: " + hashMap.size());
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(this);
                oos.close();
                elemSize = baos.size() / hashMap.size();
                System.out.println("Data Size: " + baos.size());*/
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    @Override
    public boolean containsKey(Object key) {
        if(hashMap.containsKey(key))
            return true;
        if(fastFileMap != null && fastFileMap.containsKey(key))
            return true;
        if(fastFileMapString != null && fastFileMapString.containsKey(key))
            return true;
        if (fileHashMap != null)
            return fileHashMap.containsKey(key);
        return false;
    }


    @Override
    public V get(Object key) {
        //todo fix this
        if(this.onlyDiskGet)
            return fileHashMap.get(key);
        if(fastFileMapString != null) {
            String value = fastFileMapString.get(key);
            if (value == null)
                return null;
            ArrayList<Triple> tarr = new ArrayList();
            String elems[] = value.split("ELEME_SEPERATOR");
            for (int i = 0; i < elems.length; i++) {
                String tripleStrA[] = value.split(TRIPLES_SEPERATOR );
                Triple ttriple = new Triple(Long.valueOf(tripleStrA[0]), Long.valueOf(tripleStrA[1]), Long.valueOf(tripleStrA[2]));
                tarr.add(ttriple);
            }
            return (V) tarr;
        }
        if(fastFileMap != null) {
            String v = fastFileMapString.get(key);
            if(v != null)
                return  (V)v;
        }
        if (hashMap != null && hashMap.containsKey(key))
            return hashMap.get(key);
        if (backupFileHashMap != null) {
            String value = (String) backupFileHashMap.get(key);
            if(value == null)
                return null;
            ArrayList<Triple> tarr = getArrayList(value);
            return (V) tarr;
        }
        return fileHashMap.get(key);

    }

    public ArrayList<Triple> getArrayList(String serial){
        ArrayList<Triple> tarr = new ArrayList();
        String elems[] = serial.split("ELEME_SEPERATOR");
        for (int i = 0; i < elems.length; i++) {
            String tripleStrA[] = elems[i].split(TRIPLES_SEPERATOR);
            Triple ttriple = new Triple(Long.valueOf(tripleStrA[0]), Long.valueOf(tripleStrA[1]), Long.valueOf(tripleStrA[2]));
            tarr.add(ttriple);
        }
        return  tarr;
    }


    @Override
    public V put(K key, V value) {
        setSize();
        //todo fix this
        if(diskMemPut)
            fileHashMap.put(key, value);

        /*
        if (elemSize * hashMap.size() < maxAllowedRamSize)
            return hashMap.put(key, value);*/

        if (value instanceof ArrayList) {
            ArrayList tarr = (ArrayList) value;
            if(tarr.size() <2)
                tarr.size();
            if (tarr.size() > 0 && tarr.get(0) instanceof Triple) {
                String serilaizedArray = "";
                for (int i = 0; i < tarr.size(); i++) {
                    Triple ttriple = (Triple) tarr.get(i);
                    String tripleStr = ttriple.triples[0] + TRIPLES_SEPERATOR + ttriple.triples[1] + TRIPLES_SEPERATOR + ttriple.triples[2];
                    if (i == 0)
                        serilaizedArray = tripleStr;
                    else
                        serilaizedArray = serilaizedArray + ELEME_SEPERATOR + tripleStr;
                }
                if(fastFileMapString == null){
                    File file = new File(HOME_DIR + fileName + "db.db");
                    if(file.exists())
                        file.delete();
                    org.mapdb.Serializer<K> keyType = findTypeKey(key);
                    org.mapdb.Serializer<V> valType = (Serializer<V>) Serializer.STRING;
                    fastFileMapString = createFastFileMap(keyType , valType);


                }
                if(fastFileMapString != null) {
                    return (V) fastFileMapString.put(key, serilaizedArray);
                }

                if (backupFileHashMap == null)
                    try {
                        backupFileHashMap = new FileHashMap(HOME_DIR+"backup" + fileName);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                return (V) backupFileHashMap.put(key, serilaizedArray);
            }
        }
        if (fastFileMap == null){
            fastFileMap = createFastFileMap(findTypeKey(key) , findTypeVal(value));
        }
        return fastFileMap.put(key, value);
        /*
        if (fileHashMap == null)
            try {
                fileHashMap = new FileHashMap(HOME_DIR+fileName);

            } catch (IOException e) {
                e.printStackTrace();
            }
        return fileHashMap.put(key, value);*/


    }
    private String getTripleStr(Triple triple) {
        return triple.triples[0] + TRIPLES_SEPERATOR + triple.triples[1] + TRIPLES_SEPERATOR + triple.triples[2];
    }

    public void appendToTripleList(K key, Triple triple) {
        String elem = fastFileMapString.get(key);
        String tripleStr = getTripleStr(triple);
        elem += TRIPLES_SEPERATOR+tripleStr;
        fastFileMapString.put(key,elem);
    }


    private void compress(String key , ArrayList<Triple> list){
        if(indexType == null)
            return;
        int uIndex = -1;
        for(int ii =0 ; ii < 3 ; ii++) {
            if (indexType.keyType[ii] == 1) {
                if (uIndex != -1)
                    return;
                uIndex = 0;
            }
        }

        StringBuilder stringBuilder = new StringBuilder();
        for(int i=0 ; i<list.size() ; i++) {
            stringBuilder.append()
        }
    }


    public void close(){
        try {
            if (dbFile != null)
                dbFile.close();
            if (dbMemory != null)
                dbMemory.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private HTreeMap createFastFileMap(Serializer<K> keyType , Serializer<V> valType ) {
        File file = new File(HOME_DIR + fileName + "db.db");
       if (file.exists())
            file.delete();
       if(dbFile == null) {
            dbFile = DBMaker
                    .fileDB(file)
                    .fileMmapEnable()
                    .allocateStartSize( 1 * 1024*1024*1024) // 1GB
                    .allocateIncrement(100 * 1024*1024)
                    .make();
        }
        if(dbMemory == null) {
            dbMemory = DBMaker
                    .memoryDB()
                    .make();
        }


        // Big map populated with data expired from cache
      HTreeMap onDisk = dbFile
                .hashMap("onDisk"+fileName,keyType,valType)
                .create();
            // fast in-memory collection with limited size
        HTreeMap inMemory = dbMemory
                .hashMap("inMemory"+fileName ,keyType,valType)
               // .expireStoreSize(10 * 1024*1024*1024)
                //this registers overflow to `onDisk`
              //  .expireOverflow(onDisk)
                //good idea is to enable background expiration
                .expireExecutor(Executors.newScheduledThreadPool(2))
                .create();

        return  inMemory;
 /*
          if (keyType != null && valType != null) {
            dbFile = DBMaker
                    .fileDB(HOME_DIR + fileName + "db.db")
                    .fileMmapEnable()
                    .make();
            HTreeMap map = dbFile.hashMap("map"+fileName, keyType, valType)
                    .expireStoreSize(16 * 1024*1024*1024)
                    .createOrOpen();
            return map;
        }
        return null;*/
    }

    private Serializer<K> findTypeKey(K obj) {
        if(obj instanceof  Long)
            return (Serializer<K>) Serializer.LONG;
        if(obj instanceof  Integer)
            return (Serializer<K>) Serializer.INTEGER;
        if(obj instanceof  String)
            return (Serializer<K>) Serializer.STRING;

        return null;

    }

    private Serializer<V> findTypeVal(V obj) {
        if(obj instanceof  Long)
            return (Serializer<V>) Serializer.LONG;
        if(obj instanceof  Integer)
            return (Serializer<V>) Serializer.INTEGER;
        if(obj instanceof  String)
            return (Serializer<V>) Serializer.STRING;

        return null;

    }


    @Override
    public Set<Entry<K,V>> entrySet() {
        if(fastFileMapString != null){
            ConcurrentMap<K, V> fastFileMapTT = (ConcurrentMap<K, V>) fastFileMapString;
            return fastFileMapTT.entrySet();
        }
        return hashMap.entrySet();
    }

    public Set<Entry<K,V>> fileEntrySet(){
        return fileHashMap.entrySet();
    }



    @Override
    public int size() {
        return hashMap.size()+fileHashMap.size();
    }

    public double getSizeGB(){
        return  elemSize*size()/1000000000;
    }


    public String getFileName() {
        return fileName;
    }

    public double getMemorySize() {
        if(hashMap== null)
            return 0;
        double s =  ((hashMap.size()/1000000)*elemSize)/1000;
        return s;
    }

    public void open(K key , V val) {
        org.mapdb.Serializer<K> keyType = findTypeKey(key);
        org.mapdb.Serializer<V> valType = (Serializer<V>) Serializer.STRING;
        fastFileMapString = createFastFileMap(keyType , valType);
    }

    
}

