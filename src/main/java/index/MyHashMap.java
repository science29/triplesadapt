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

    private HashMap<K, V> hashMap = new HashMap<K, V>();
    private double elemSize = 0;

    private double maxAllowedRamSize ;
    private final long fac = 16;
    private final long MAX_SIZE_GB =  fac*1000000000;
    private long n = 1;
    private final long GB =  n*1000000000;

    public  IndexType indexType ;
    public  IndexType extraIndexType = null;

    public static final int [] noKeyType = {0,0,0};

    public boolean onlyDiskGet = false;
    public boolean diskMemPut = false;



    private final String HOME_DIR = "/home/ahmed/";
    private DB dbMemory;
    private boolean comressEnabled = false;
    private boolean cacheEnabled = true;


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
        comressEnabled = false;
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
        comressEnabled = false;
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
        if(indexType.keyType[0]+indexType.keyType[1]+indexType.keyType[2] == 1)
            comressEnabled = true;
        else
            comressEnabled = false;
    }

    public MyHashMap(String fileName, IndexType indexType , boolean cacheEnabled) {
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
        this.cacheEnabled = cacheEnabled;
        if(indexType.keyType[0]+indexType.keyType[1]+indexType.keyType[2] == 1)
            comressEnabled = true;
        else
            comressEnabled = false;
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
                return getFromCache(key);
            ArrayList<Triple> tarr;
            if(value.startsWith("c")) {
                if(extraIndexType == null)
                    tarr = deCompress( value);
                else
                    //tarr = specialDeCompress( value);
                    tarr = deSerializeArrayList(value);
                return (V) tarr;
            }

            tarr = deSerializeArrayList(value);
            /*String elems[] = value.split("ELEME_SEPERATOR");
            for (int i = 0; i < elems.length; i++) {
                String tripleStrA[] = value.split(TRIPLES_SEPERATOR );
                Triple ttriple = new Triple(Long.valueOf(tripleStrA[0]), Long.valueOf(tripleStrA[1]), Long.valueOf(tripleStrA[2]));
                tarr.add(ttriple);
            }*/
            return (V) tarr;
        }
        if(fastFileMap != null) {
            V v = fastFileMap.get(key);
            if(v != null)
                return  v;
        }
        if (hashMap != null && hashMap.containsKey(key))
            return hashMap.get(key);
        if (backupFileHashMap != null) {
            String value = (String) backupFileHashMap.get(key);
            if(value == null)
                return getFromCache(key);
            ArrayList<Triple> tarr = getArrayList(value);
            return (V) tarr;
        }
        return fileHashMap.get(key);

    }



    public static ArrayList<Triple> deSerializeArrayList(String value) {
        ArrayList <Triple> tarr = new ArrayList();
        String elems[] = value.split(ELEME_SEPERATOR);
        for (int i = 0; i < elems.length; i++) {
            String tripleStrA[] = elems[i].split(TRIPLES_SEPERATOR );
            Triple ttriple = new Triple(Long.valueOf(tripleStrA[0]), Long.valueOf(tripleStrA[1]), Long.valueOf(tripleStrA[2]));
            tarr.add(ttriple);
        }
        return tarr;
    }


    public ArrayList<Triple> getArrayList(String serial){
        return deSerializeArrayList(serial);
        /*ArrayList<Triple> tarr = new ArrayList();
        String elems[] = serial.split("ELEME_SEPERATOR");
        for (int i = 0; i < elems.length; i++) {
            String tripleStrA[] = elems[i].split(TRIPLES_SEPERATOR);
            Triple ttriple = new Triple(Long.valueOf(tripleStrA[0]), Long.valueOf(tripleStrA[1]), Long.valueOf(tripleStrA[2]));
            tarr.add(ttriple);
        }
        return  tarr;*/
    }


    @Override
    public V put(K key, V value) {
        setSize();
        //todo fix this
        if(diskMemPut)
            fileHashMap.put(key, value);

        if(addToCache(key,value))
            return value;
        /*
        if (elemSize * hashMap.size() < maxAllowedRamSize)
            return hashMap.put(key, value);*/
        if (value instanceof ArrayList) {
            ArrayList tarr = (ArrayList) value;
            if (tarr.size() > 0 && tarr.get(0) instanceof Triple) {
                String serilaizedArray = "";
                if(comressEnabled && tarr.size() > 100){
                    if(extraIndexType == null) {
                        serilaizedArray = compress(tarr);
                      //  tempCheck(tarr, deCompress(serilaizedArray));
                    }
                    else {
                        serilaizedArray = serializeArrayList(tarr);
                        /*serilaizedArray = specialCompress(tarr);
                        tempCheck(tarr,specialDeCompress(serilaizedArray));*/
                    }

                   // if(serilaizedArray.contains(":"))

                }else
                    serilaizedArray = serializeArrayList(tarr);
                if(fastFileMapString == null){
                   /* File file = new File(HOME_DIR + fileName + "db.db");
                    if(file.exists())
                        file.delete();*/
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

    private V putArrayList(K key , ArrayList<Triple> tarr , boolean append){
        if (tarr.size() > 0 && tarr.get(0) instanceof Triple) {
            String serilaizedArray = "";
            if (comressEnabled && tarr.size() > 100) {
                if (extraIndexType == null) {
                    serilaizedArray = compress(tarr);
                    //  tempCheck(tarr, deCompress(serilaizedArray));
                } else {
                    serilaizedArray = serializeArrayList(tarr);
                        /*serilaizedArray = specialCompress(tarr);
                        tempCheck(tarr,specialDeCompress(serilaizedArray));*/
                }

                // if(serilaizedArray.contains(":"))

            } else
                serilaizedArray = serializeArrayList(tarr);
            if (fastFileMapString == null) {
                org.mapdb.Serializer<K> keyType = findTypeKey(key);
                org.mapdb.Serializer<V> valType = (Serializer<V>) Serializer.STRING;
                fastFileMapString = createFastFileMap(keyType, valType);
            }
            if (fastFileMapString != null) {
                if(!append)
                    return (V) fastFileMapString.put(key, serilaizedArray);
                String val = fastFileMapString.get(key);
                if(val == null)
                    return (V) fastFileMapString.put(key, serilaizedArray);
                val = val +ELEME_SEPERATOR+serilaizedArray;xxx
            }
        }
            return null;
        }



    private V getFromCache(Object key) {
        if(cacheEnabled)
            return hashMap.get(key);
        return null;
    }

    private boolean addToCache(K key ,V value){
        if(cacheEnabled){
            hashMap.put(key, value);
            if(hashMap.size() > 1000000){
                writeCacheToPersist();
            }
            return true;
        }
        return false;
    }

    public void addTripletoList(Long key, Triple triple) {
        V val = getFromCache(key);
        if(val != null){
            ArrayList<Triple>  arr = (ArrayList<Triple>) val;
            arr.add(triple);
        }else {
            ArrayList<Triple> arr =new ArrayList<Triple>();
            arr.add(triple);
            put((K) key , (V)arr);
        }
    }

    public void writeCacheToPersist() {
        if(hashMap == null || !cacheEnabled)
            return;
        System.out.println("writing "+fileName+" cache ..");
        cacheEnabled = false;
        Iterator it = hashMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            K key = (K) pair.getKey();
            V val = (V) pair.getValue();
            put(key , val);
        }
        hashMap = new HashMap<K, V>();
        cacheEnabled = true;
    }

    public static String serializeArrayList(ArrayList<Triple> tarr){
        StringBuilder serilaizedArray = new StringBuilder();
        for (int i = 0; i < tarr.size(); i++) {
            Triple ttriple =  tarr.get(i);
            if (i != 0)
                serilaizedArray.append(ELEME_SEPERATOR);
            serilaizedArray.append(ttriple.triples[0]); serilaizedArray.append(TRIPLES_SEPERATOR); serilaizedArray.append(ttriple.triples[1]); serilaizedArray.append(TRIPLES_SEPERATOR);serilaizedArray.append(ttriple.triples[2]);
                /* String tripleStr = ttriple.triples[0] + TRIPLES_SEPERATOR + ttriple.triples[1] + TRIPLES_SEPERATOR + ttriple.triples[2];
            if (i == 0)
                serilaizedArray = tripleStr;
            else
                serilaizedArray = serilaizedArray + ELEME_SEPERATOR + tripleStr;*/
        }
        return serilaizedArray.toString();
    }



    public static String genSerializeArrayList(ArrayList<MySerialzable> tarr){
        StringBuilder serilaizedArray = new StringBuilder();
        for (int i = 0; i < tarr.size(); i++) {
            MySerialzable mySerialzable = tarr.get(i);
            String str = mySerialzable.serialize();
            if (i != 0)
                serilaizedArray.append(ELEME_SEPERATOR);
            serilaizedArray.append(str);
        }
        return serilaizedArray.toString();
    }

    public static String[] genDeSerializeArrayList(String res ){
         return res.split(ELEME_SEPERATOR);
    }




    private void tempCheck(ArrayList<Triple> tarr, ArrayList<Triple> chList) {

        Collections.sort(tarr, new Comparator<Triple>() {
            @Override
            public int compare(Triple lhs, Triple rhs) {
                // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                if(lhs.triples[0] < rhs.triples[0] )
                    return -1;
                if(lhs.triples[0] > rhs.triples[0] )
                    return 1;
                return 0;
            }
        });


        for(int i = 0; i<tarr.size() ; i++){
            if(tarr.get(i).triples[0] == chList.get(i).triples[0] && tarr.get(i).triples[1] == chList.get(i).triples[1] && tarr.get(i).triples[2] == chList.get(i).triples[2])
                continue;
            else {
                System.out.println(tarr.get(i).triples[0] +" == "+  chList.get(i).triples[0] +" && "+ tarr.get(i).triples[1]+" == "+chList.get(i).triples[1] +" && "+ tarr.get(i).triples[2] +" == "+chList.get(i).triples[2]);
                return;
            }

        }
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


    private String compress( ArrayList<Triple> list){
        if(indexType == null)
            return null;
        int uIndex = -1;
        for(int ii =0 ; ii < 3 ; ii++) {
            if (indexType.keyType[ii] == 1) {
                if (uIndex != -1)
                    return null;
                uIndex = ii;
            }
        }

        final int tindex = uIndex;
        //sort
        Collections.sort(list, new Comparator<Triple>() {
            @Override
            public int compare(Triple lhs, Triple rhs) {
                // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                if(lhs.triples[tindex] < rhs.triples[tindex] )
                    return -1;
                if(lhs.triples[tindex] > rhs.triples[tindex] )
                    return 1;
                return 0;
            }
        });

        StringBuilder stringBuilder = new StringBuilder("c");
        stringBuilder.append(list.get(0).triples[0]);stringBuilder.append("c");
        stringBuilder.append(list.get(0).triples[1]);stringBuilder.append("c");
        stringBuilder.append(list.get(0).triples[2]);stringBuilder.append("c");
        long prev = list.get(0).triples[uIndex];
        long base = prev;
        boolean saving = false;
        for(int i=1 ; i<list.size() ; i++) {
            long now = list.get(i).triples[uIndex];
            if(now-prev == 1 && i < list.size() - 1 && i>1) {
                saving = true;
                prev = now;
                continue;
            }
            else {
                if(saving){
                    stringBuilder.append(":");
                    stringBuilder.append(prev-base);
                    saving = false;
                }
                stringBuilder.append("a");
                stringBuilder.append(now-base);
            }
            prev = now;
        }
        return stringBuilder.toString();
    }




    private String specialCompress(ArrayList<Triple> list) {
        ArrayList<Triple> list1 = new ArrayList<Triple>();
        ArrayList<Triple> list2 = new ArrayList<Triple>();
        for(int i=0; i<list.size() ; i+=2){
            list1.add(list.get(i));
            list2.add(list.get(i+1));
        }

        ArrayList res = new ArrayList();
        for (int i = 0; i < list1.size(); i++){
            res.add(list1.get(i));
            res.add(list2.get(i));
        }
      /*  tempCheck(list, res);

        String s1 = compress(list1 );
        this.s1 = s1;
        tempCheck(list1, deCompress(s1));
        String s2 = serializeArrayList(list2 );
        this.s2 = s2;
        tempCheck(list2, deSerializeArrayList(s2));
        String st = s1+"n"+s2;
        tempCheck(list,specialDeCompress(st));*/
        return s1+"n"+s2;
    }

    String s1,s2;
    private ArrayList<Triple> specialDeCompress(String value) {
        String[] ar = value.split("n");
        if(!s1.matches(ar[0]))
            s1.matches(" ");
        if(!s2.matches(ar[1]))
            s2.matches(" ");
        ArrayList res = new ArrayList();
        ArrayList<Triple> list1 = deCompress(ar[0]);
        ArrayList<Triple> list2 = deSerializeArrayList(ar[1]);
        for (int i = 0; i < list1.size(); i++){
            res.add(list1.get(i));
            res.add(list2.get(i));
        }
        return res;
    }

    private ArrayList<Triple> deCompress(String record){
        return deCompress(record,null);
    }
    private ArrayList<Triple> deCompress(String record ,  ArrayList<Triple> resLsit){
        if(!record.startsWith("c"))
            return null;

        //first solve the case of more than one compressed arrays
        String [] compressedRecordArrs  = record.split("k");
        if(compressedRecordArrs.length > 1) {
            resLsit = new ArrayList<Triple>();
            for (int p = 0; p < compressedRecordArrs.length; p++) {
                deCompress(compressedRecordArrs[p], resLsit);
            }
            return resLsit;
        }

        int uIndex = -1;
        int firstFixedIndex = -1;
        int secondFixedIndex = -1;
        for(int ii =0 ; ii < 3 ; ii++) {
            if (indexType.keyType[ii] == 1) {
                if (uIndex != -1)
                    return null;
                uIndex = ii;
            }else if(firstFixedIndex == -1)
                firstFixedIndex = ii;
            else
                secondFixedIndex = ii;
        }
        String [] arrIni = record.split("c");
        Triple triple = new Triple(Long.valueOf(arrIni[1]) , Long.valueOf(arrIni[2]) ,Long.valueOf(arrIni[3]));
        if(resLsit == null)
            resLsit = new ArrayList<Triple>();
        resLsit.add(triple);
        String [] arr = record.split("a");
        long base = triple.triples[uIndex];
        for(int i = 1 ; i < arr.length ; i++){
            try{
                long val =  Long.valueOf(arr[i]);
                Triple triple1 = new Triple(0,0, 0);
                triple1.triples[uIndex] = val+base;
                triple1.triples[firstFixedIndex] = triple.triples[firstFixedIndex];
                triple1.triples[secondFixedIndex] = triple.triples[secondFixedIndex];
                resLsit.add(triple1);
            }catch (NumberFormatException e){
                String [] arr2 = arr[i].split(":");
                long from = Long.valueOf(arr2[0])+base;
                long to = Long.valueOf(arr2[1])+base;
                for( ; from<=to ; from++){
                    Triple triple1 = new Triple(0,0, 0);
                    triple1.triples[uIndex] = from;
                    triple1.triples[firstFixedIndex] = triple.triples[firstFixedIndex];
                    triple1.triples[secondFixedIndex] = triple.triples[secondFixedIndex];
                    resLsit.add(triple1);
                }
            }

        }
        return resLsit;
    }


    public void close(){
        try {
            if (dbFile != null) {
                dbFile.commit();
                dbFile.close();
            }
            if (dbMemory != null) {
                dbMemory.commit();
                dbMemory.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    private HTreeMap createFastFileMap(Serializer<K> keyType , Serializer<V> valType ) {
        File file = new File(HOME_DIR + fileName + "db.db");
       /*if (file.exists())
            file.delete();*/
       if(dbFile == null|| dbFile.isClosed()) {
            dbFile = DBMaker
                    .fileDB(file)
                    .fileMmapEnable()
                    .allocateStartSize( 1000 * 1024*1024) // 1GB
                    .allocateIncrement(100 * 1024*1024)
                    .make();
        }
        if(dbMemory == null || dbMemory.isClosed()) {
            dbMemory = DBMaker
                    .memoryDB()
                    .make();
        }


        // Big map populated with data expired from cache
      HTreeMap onDisk = dbFile
                .hashMap("onDisk"+fileName,keyType,valType)
                .createOrOpen();
            // fast in-memory collection with limited size
        HTreeMap inMemory = dbMemory
                .hashMap("inMemory"+fileName ,keyType,valType)
                .expireStoreSize(500 *1024*1024)
                //this registers overflow to `onDisk`
                .expireOverflow(onDisk)
                //good idea is to enable background expiration
                .expireExecutor(Executors.newScheduledThreadPool(2))
                .createOrOpen();

        return  onDisk;
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

    public void commitToDisk(){
        dbMemory.commit();
        dbFile.commit();
        fastFileMapString = null;
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

    public Set<Entry<K,String>> fastEntrySet(){
        return fastFileMapString.entrySet();
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

    public void setExtraIndexType(IndexType extraIndexType) {
        this.extraIndexType = extraIndexType;
    }



}

