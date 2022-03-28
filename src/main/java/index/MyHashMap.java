package index;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import sun.security.krb5.internal.ktab.KeyTab;
import triple.Triple;
import triple.TriplePattern2;
import util.FileHashMap;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;


//TODO solve the  creation of the filehashmap
public class MyHashMap<K, V> extends HashMap<K, V> implements Serializable {

    private static final String ELEME_SEPERATOR = "e";
    private static final String TRIPLES_SEPERATOR = "t";
    private static final String HYPRYID_SEPEAROTR = "h";
    private static final String COMPRESSED_SEPERATOR = "c";
    private String fileName;
    ConcurrentMap<K, V> fastFileMap;
    private ConcurrentMap<K, String> fastFileMapString;
    DB dbFile;


    private FileHashMap<K, V> fileHashMap;
    private FileHashMap<K, String> backupFileHashMap;

    private HashMap<K, V> hashMap = new HashMap<K, V>();
    private double elemSize = 0;

    private double maxAllowedRamSize;
    private final int fac = 16;
    private final int MAX_SIZE_GB = fac * 1000000000;
    private int n = 1;
    private final int GB = n * 1000000000;

    public IndexType indexType;
    public IndexType extraIndexType = null;

    public static final int[] noKeyType = {0, 0, 0};

    public boolean onlyDiskGet = false;
    public boolean diskMemPut = false;
    private boolean onlyMem = true;
    private boolean sorted = false;

    private final String HOME_DIR = "/home/ahmed/";
    private DB dbMemory;
    private boolean comressEnabled = false;
    private boolean cacheEnabled = true;
    private MyHashMap<K, V>.Consumer consumer;
    private HashMap<K, V> queryTimeCache;
    public boolean queryTimeCacheEnabled = false;
    public byte poolRefType;


    public boolean isSorted() {
        return sorted;
    }

    public void sort(final int index1, final int index2) {
        //iterate on all values
        sorted = true;
        Iterator it = hashMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            pair.getKey();
            ArrayList<Triple> tripleList = (ArrayList<Triple>) pair.getValue();
            sortArray(tripleList,index1,index2);

        }
    }


    public static void sortArray(ArrayList<Triple> tripleList , int index1 , int index2){

        Collections.sort(tripleList, new Comparator<Triple>() {
            // @Override
            public int compare(Triple lhs, Triple rhs) {
                // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                if (lhs.triples[index1] < rhs.triples[index1])
                    return -1;
                if (lhs.triples[index1] > rhs.triples[index1])
                    return 1;
                if (index2 == -1)
                    return 0;
                if (lhs.triples[index2] < rhs.triples[index2])
                    return -1;
                if (lhs.triples[index2] > rhs.triples[index2])
                    return 1;
                return 0;
                // return lhs.customInt > rhs.customInt ? -1 : (lhs.customInt < rhs.customInt) ? 1 : 0;
            }
        });
    }




    public MyHashMap(String name, HashMap<K, V> map) {
        super();
        if (!onlyMem) {
            try {
                fileHashMap = new FileHashMap(HOME_DIR + fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.fileName = name;
        }
        hashMap = map;

        this.indexType = new IndexType();
        maxAllowedRamSize = MAX_SIZE_GB;
        comressEnabled = false;
    }


    //   private int avgElemSize = 1 ;
    public MyHashMap(String fileName) {
        super();
        if (!onlyMem) {
            try {
                fileHashMap = new FileHashMap(HOME_DIR + fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.fileName = fileName;
        }
        hashMap = new HashMap();
        this.indexType = new IndexType();
        maxAllowedRamSize = MAX_SIZE_GB;
        comressEnabled = false;
    }

    public MyHashMap(String fileName, double memSizeGB) {
        super();
        try {
            fileHashMap = new FileHashMap(HOME_DIR + fileName);
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
            fileHashMap = new FileHashMap(HOME_DIR + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashMap = new HashMap();
        this.fileName = fileName;
        this.indexType = indexType;
        maxAllowedRamSize = MAX_SIZE_GB;
        if (indexType.keyType[0] + indexType.keyType[1] + indexType.keyType[2] == 1)
            comressEnabled = true;
        else
            comressEnabled = false;
    }

    public MyHashMap(String fileName, IndexType indexType, boolean cacheEnabled) {
        super();
        try {
            fileHashMap = new FileHashMap(HOME_DIR + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashMap = new HashMap();
        this.fileName = fileName;
        this.indexType = indexType;
        maxAllowedRamSize = MAX_SIZE_GB;
        this.cacheEnabled = cacheEnabled;
        if (indexType.keyType[0] + indexType.keyType[1] + indexType.keyType[2] == 1)
            comressEnabled = true;
        else
            comressEnabled = false;
    }


    private void setSize() {
        try {
            if (fastFileMap == null || fastFileMap.size() != 40000 || elemSize != 0)
                return;

            FileWriter fileWriter = new FileWriter(HOME_DIR + "temp_shjf");
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            int cnt = 0;
            Iterator it = hashMap.entrySet().iterator();
            while (it.hasNext()) {
                if (cnt > 40000)
                    break;
                Map.Entry pair = (Map.Entry) it.next();
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
            File file = new File(HOME_DIR + "temp_shjf");
            if (file.length() < 100000000)
                file.length();
            elemSize = (int) file.length() / 40000;
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
    public V remove(Object key) {
        return hashMap.remove(key);
    }


    @Override
    public boolean containsKey(Object key) {
        if(onlyMem)
            return hashMap.containsKey(key);
        if (hashMap.containsKey(key))
            return true;
        if (fastFileMap != null && fastFileMap.containsKey(key))
            return true;
        if (fastFileMapString != null && fastFileMapString.containsKey(key))
            return true;
        if (fileHashMap != null)
            return fileHashMap.containsKey(key);
        return false;
    }


    @Override
    public V get(Object key) {
        //todo fix this
        if (onlyMem)
            return this.hashMap.get(key);
        if (this.onlyDiskGet)
            return fileHashMap.get(key);
        if (queryTimeCacheEnabled) {
            V cVal = getFromQueryTimeCache(key);
            if (cVal != null)
                return cVal;
        }
        if (fastFileMapString != null) {
            String value = fastFileMapString.get(key);
            if (value == null)
                return getFromCache(key);
            ArrayList<Triple> tarr = deCompressedHyprid(value);
            return (V) tarr;
        }
        if (fastFileMap != null) {
            V v = fastFileMap.get(key);
            if (v != null)
                return v;
        }
        if (hashMap != null && hashMap.containsKey(key))
            return hashMap.get(key);
        if (backupFileHashMap != null) {
            String value = (String) backupFileHashMap.get(key);
            if (value == null)
                return getFromCache(key);
            ArrayList<Triple> tarr = getArrayList(value);
            return (V) tarr;
        }
        return fileHashMap.get(key);

    }

    public V get(Integer key1, Integer key2, int sortedIndex, TriplePattern2.WithinIndex withinIndex) {
        ArrayList<Triple> triples = (ArrayList<Triple>) get(key1);
        if (triples == null)
            return null;
        int cost = binarySearch(triples, sortedIndex, key2, withinIndex);
        if (cost == -1)
            return null;
        withinIndex.cost = cost;
        if(key2 > 0)
            withinIndex.potentailFilterCost = triples.size();
        return (V) triples;
    }


    public static ArrayList<Triple> deSerializeArrayList(String value, ArrayList<Triple> tarr) {
        if (tarr == null)
            tarr = new ArrayList();
        String elems[] = value.split(ELEME_SEPERATOR);
        for (int i = 0; i < elems.length; i++) {
            String tripleStrA[] = elems[i].split(TRIPLES_SEPERATOR);
            Triple ttriple = new Triple(Integer.valueOf(tripleStrA[0]), Integer.valueOf(tripleStrA[1]), Integer.valueOf(tripleStrA[2]));
            tarr.add(ttriple);
        }
        return tarr;
    }


    public ArrayList<Triple> getArrayList(String serial) {
        return deSerializeArrayList(serial, null);
        /*ArrayList<Triple> tarr = new ArrayList();
        String elems[] = serial.split("ELEME_SEPERATOR");
        for (int i = 0; i < elems.length; i++) {
            String tripleStrA[] = elems[i].split(TRIPLES_SEPERATOR);
            Triple ttriple = new Triple(Integer.valueOf(tripleStrA[0]), Integer.valueOf(tripleStrA[1]), Integer.valueOf(tripleStrA[2]));
            tarr.add(ttriple);
        }
        return  tarr;*/
    }


    @Override
    public V put(K key, V value) {
        if(!onlyMem) {
            setSize();
            //todo fix this
            if (diskMemPut)
                fileHashMap.put(key, value);
        }
        if (addToCache(key, value))
            return value;
        /*
        if (elemSize * hashMap.size() < maxAllowedRamSize)
            return hashMap.put(key, value);*/
        if (value instanceof ArrayList) {
            ArrayList tarr = (ArrayList) value;
            if (tarr.size() > 0 && tarr.get(0) instanceof Triple) {
                putArrayList(key, tarr, true);
               /*
                if (backupFileHashMap == null)
                    try {
                        backupFileHashMap = new FileHashMap(HOME_DIR+"backup" + fileName);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                return (V) backupFileHashMap.put(key, serilaizedArray);*/
            }
            return value;
        }
        if (fastFileMap == null) {
            fastFileMap = createFastFileMap(findTypeKey(key), findTypeVal(value));
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

    private String putArrayList(K key, ArrayList<Triple> tarr, boolean append) {
        if (tarr.size() > 0 && tarr.get(0) instanceof Triple) {
            String serilaizedArray = "";
            boolean weCompress = false;
            if (comressEnabled && tarr.size() > 100) {
                if (extraIndexType == null) {
                    serilaizedArray = compress(tarr);
                    weCompress = true;
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
                if (!append)
                    return fastFileMapString.put(key, serilaizedArray);
                String val = fastFileMapString.get(key);
                if (val == null)
                    return fastFileMapString.put(key, serilaizedArray);
                if (!weCompress)
                    val = val + ELEME_SEPERATOR + serilaizedArray;
                else
                    val = val + HYPRYID_SEPEAROTR + serilaizedArray;
                return fastFileMapString.put(key, val);
            }
        }
        return null;
    }


    private V getFromCache(Object key) {
        if (cacheEnabled)
            return hashMap.get(key);
        return null;
    }

    private V getFromQueryTimeCache(Object key) {
        if (queryTimeCache != null)
            return queryTimeCache.get(key);
        return null;
    }

    private int maxCacheSize = 10000000;
    private int minCacheSize = 500000;
    private double factor = 2;

    private boolean addToCache(K key, V value) {
        if (cacheEnabled) {
            hashMap.put(key, value);
            if (!onlyMem && hashMap.size() > maxCacheSize) {
                writeCacheToPersist();
                maxCacheSize = (int) (((double) maxCacheSize) / factor);
                factor = factor - 0.18;
                if (maxCacheSize < minCacheSize)
                    maxCacheSize = minCacheSize;
            }
            return true;
        }
        return false;
    }


    public void addTripleLazy(K key, Triple triple) {
        if (onlyMem) {
            addTripletoList(key, triple);
            return;
        }
        //waring no get is allowed
        if (consumer == null) {
            consumer = new Consumer();
            consumer.start();
        }
        consumer.addTriple(key, triple);
    }

    public void addTripletoList(K key, Triple triple) {
        V val = getFromCache(key);
        if (val != null) {
            ArrayList<Triple> arr = (ArrayList<Triple>) val;
            arr.add(triple);
        } else {
            ArrayList<Triple> arr = new ArrayList<Triple>();
            arr.add(triple);
            put((K) key, (V) arr);
        }
    }

    public void writeCacheToPersist() {
        if (hashMap == null || !cacheEnabled)
            return;
        System.out.println("writing " + fileName + " cache ..");
        cacheEnabled = false;
        Iterator it = hashMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            K key = (K) pair.getKey();
            V val = (V) pair.getValue();
            put(key, val);
        }
        hashMap = new HashMap<K, V>();
        cacheEnabled = true;
    }

    public static String serializeArrayList(ArrayList<Triple> tarr) {
        StringBuilder serilaizedArray = new StringBuilder();
        for (int i = 0; i < tarr.size(); i++) {
            Triple ttriple = tarr.get(i);
            if (i != 0)
                serilaizedArray.append(ELEME_SEPERATOR);
            serilaizedArray.append(ttriple.triples[0]);
            serilaizedArray.append(TRIPLES_SEPERATOR);
            serilaizedArray.append(ttriple.triples[1]);
            serilaizedArray.append(TRIPLES_SEPERATOR);
            serilaizedArray.append(ttriple.triples[2]);
                /* String tripleStr = ttriple.triples[0] + TRIPLES_SEPERATOR + ttriple.triples[1] + TRIPLES_SEPERATOR + ttriple.triples[2];
            if (i == 0)
                serilaizedArray = tripleStr;
            else
                serilaizedArray = serilaizedArray + ELEME_SEPERATOR + tripleStr;*/
        }
        return serilaizedArray.toString();
    }


    public static String genSerializeArrayList(ArrayList<MySerialzable> tarr) {
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

    public static String[] genDeSerializeArrayList(String res) {
        return res.split(ELEME_SEPERATOR);
    }


    private void tempCheck(ArrayList<Triple> tarr, ArrayList<Triple> chList) {

        Collections.sort(tarr, new Comparator<Triple>() {

            public int compare(Triple lhs, Triple rhs) {
                // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                if (lhs.triples[0] < rhs.triples[0])
                    return -1;
                if (lhs.triples[0] > rhs.triples[0])
                    return 1;
                return 0;
            }
        });


        for (int i = 0; i < tarr.size(); i++) {
            if (tarr.get(i).triples[0] == chList.get(i).triples[0] && tarr.get(i).triples[1] == chList.get(i).triples[1] && tarr.get(i).triples[2] == chList.get(i).triples[2])
                continue;
            else {
                System.out.println(tarr.get(i).triples[0] + " == " + chList.get(i).triples[0] + " && " + tarr.get(i).triples[1] + " == " + chList.get(i).triples[1] + " && " + tarr.get(i).triples[2] + " == " + chList.get(i).triples[2]);
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
        elem += TRIPLES_SEPERATOR + tripleStr;
        fastFileMapString.put(key, elem);
    }


    private String compress(ArrayList<Triple> list) {
        if (indexType == null)
            return null;
        int uIndex = -1;
        for (int ii = 0; ii < 3; ii++) {
            if (indexType.keyType[ii] == 1) {
                if (uIndex != -1)
                    return null;
                uIndex = ii;
            }
        }

        final int tindex = uIndex;
        //sort
        Collections.sort(list, new Comparator<Triple>() {

            public int compare(Triple lhs, Triple rhs) {
                // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                if (lhs.triples[tindex] < rhs.triples[tindex])
                    return -1;
                if (lhs.triples[tindex] > rhs.triples[tindex])
                    return 1;
                return 0;
            }
        });

        StringBuilder stringBuilder = new StringBuilder("c");
        stringBuilder.append(list.get(0).triples[0]);
        stringBuilder.append("c");
        stringBuilder.append(list.get(0).triples[1]);
        stringBuilder.append("c");
        stringBuilder.append(list.get(0).triples[2]);
        stringBuilder.append("c");
        int prev = list.get(0).triples[uIndex];
        int base = prev;
        boolean saving = false;
        for (int i = 1; i < list.size(); i++) {
            int now = list.get(i).triples[uIndex];
            if (now - prev == 1 && i < list.size() - 1 && i > 1) {
                saving = true;
                prev = now;
                continue;
            } else {
                if (saving) {
                    stringBuilder.append(":");
                    stringBuilder.append(prev - base);
                    saving = false;
                }
                stringBuilder.append("a");
                stringBuilder.append(now - base);
            }
            prev = now;
        }
        return stringBuilder.toString();
    }


    private String specialCompress(ArrayList<Triple> list) {
        ArrayList<Triple> list1 = new ArrayList<Triple>();
        ArrayList<Triple> list2 = new ArrayList<Triple>();
        for (int i = 0; i < list.size(); i += 2) {
            list1.add(list.get(i));
            list2.add(list.get(i + 1));
        }

        ArrayList res = new ArrayList();
        for (int i = 0; i < list1.size(); i++) {
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
        return s1 + "n" + s2;
    }

    String s1, s2;

    private ArrayList<Triple> specialDeCompress(String value) {
        String[] ar = value.split("n");
        if (!s1.matches(ar[0]))
            s1.matches(" ");
        if (!s2.matches(ar[1]))
            s2.matches(" ");
        ArrayList res = new ArrayList();
        ArrayList<Triple> list1 = deCompress(ar[0]);
        ArrayList<Triple> list2 = deSerializeArrayList(ar[1], null);
        for (int i = 0; i < list1.size(); i++) {
            res.add(list1.get(i));
            res.add(list2.get(i));
        }
        return res;
    }


    public ArrayList<Triple> deCompressedHyprid(String record) {
        if (record.contains(HYPRYID_SEPEAROTR)) {
            String[] arr = record.split(HYPRYID_SEPEAROTR);
            ArrayList<Triple> res = new ArrayList<Triple>();
            for (int i = 0; i < arr.length; i++) {
                if (arr[i].startsWith(COMPRESSED_SEPERATOR)) {
                    deCompress(record, res);
                } else {
                    deSerializeArrayList(arr[i], res);
                }
            }
            return res;
        }
        if (record.startsWith(COMPRESSED_SEPERATOR)) {
            return deCompress(record);
        }
        return deSerializeArrayList(record, null);

    }

    private ArrayList<Triple> deCompress(String record) {
        return deCompress(record, null);
    }

    private ArrayList<Triple> deCompress(String record, ArrayList<Triple> resLsit) {
        if (!record.startsWith(COMPRESSED_SEPERATOR))
            return null;

        //first solve the case of more than one compressed arrays
        String[] compressedRecordArrs = record.split("k");
        if (compressedRecordArrs.length > 1) {
            resLsit = new ArrayList<Triple>();
            for (int p = 0; p < compressedRecordArrs.length; p++) {
                deCompress(compressedRecordArrs[p], resLsit);
            }
            return resLsit;
        }

        int uIndex = -1;
        int firstFixedIndex = -1;
        int secondFixedIndex = -1;
        for (int ii = 0; ii < 3; ii++) {
            if (indexType.keyType[ii] == 1) {
                if (uIndex != -1)
                    return null;
                uIndex = ii;
            } else if (firstFixedIndex == -1)
                firstFixedIndex = ii;
            else
                secondFixedIndex = ii;
        }
        String[] arrIni = record.split(COMPRESSED_SEPERATOR);
        Triple triple = new Triple(Integer.valueOf(arrIni[1]), Integer.valueOf(arrIni[2]), Integer.valueOf(arrIni[3]));
        if (resLsit == null)
            resLsit = new ArrayList<Triple>();
        resLsit.add(triple);
        String[] arr = record.split("a");
        int base = triple.triples[uIndex];
        for (int i = 1; i < arr.length; i++) {
            try {
                int val = Integer.valueOf(arr[i]);
                Triple triple1 = new Triple(0, 0, 0);
                triple1.triples[uIndex] = val + base;
                triple1.triples[firstFixedIndex] = triple.triples[firstFixedIndex];
                triple1.triples[secondFixedIndex] = triple.triples[secondFixedIndex];
                resLsit.add(triple1);
            } catch (NumberFormatException e) {
                String[] arr2 = arr[i].split(":");
                int from = Integer.valueOf(arr2[0]) + base;
                int to = Integer.valueOf(arr2[1]) + base;
                for (; from <= to; from++) {
                    Triple triple1 = new Triple(0, 0, 0);
                    triple1.triples[uIndex] = from;
                    triple1.triples[firstFixedIndex] = triple.triples[firstFixedIndex];
                    triple1.triples[secondFixedIndex] = triple.triples[secondFixedIndex];
                    resLsit.add(triple1);
                }
            }

        }
        return resLsit;
    }


    public void close() {
        try {
            writeCacheToPersist();
            if (dbFile != null) {
                dbFile.commit();
                dbFile.close();
            }
            if (dbMemory != null) {
                dbMemory.commit();
                dbMemory.close();
            }
            if (consumer != null)
                consumer.stopThread();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public int getWritingThreadBufferSize() {
        if (consumer != null)
            return consumer.getBufferSize();
        return 0;
    }

    private HTreeMap createFastFileMap(Serializer<K> keyType, Serializer<V> valType) {
        File file = new File(HOME_DIR + fileName + "db.db");
       /*if (file.exists())
            file.delete();*/

        if (dbFile == null || dbFile.isClosed()) {
            dbFile = DBMaker
                    .fileDB(file)
                    .fileMmapEnable()
                    .closeOnJvmShutdown()
                    .allocateStartSize(1000 * 1024 * 1024) // 1GB
                    .allocateIncrement(100 * 1024 * 1024)
                    //.transactionEnable()
                    .make();
        }
        if (dbMemory == null || dbMemory.isClosed()) {
            dbMemory = DBMaker
                    .memoryDB()
                    .make();
        }


        // Big map populated with data expired from cache
        HTreeMap onDisk = dbFile
                .hashMap("onDisk" + fileName, keyType, valType)
                .createOrOpen();
        // fast in-memory collection with limited size
        HTreeMap inMemory = dbMemory
                .hashMap("fastMap" + fileName, keyType, valType)
                .expireStoreSize(500 * 1024 * 1024)
                //this registers overflow to `onDisk`
                .expireOverflow(onDisk)
                //good idea is to enable background expiration
                .expireExecutor(Executors.newScheduledThreadPool(2))
                .createOrOpen();

        return onDisk;
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

    public void commitToDisk() {
        dbMemory.commit();
        dbFile.commit();
        // fastFileMapString = null;
    }

    private Serializer<K> findTypeKey(K obj) {
        if (obj instanceof Integer)
            return (Serializer<K>) Serializer.LONG;
        if (obj instanceof Integer)
            return (Serializer<K>) Serializer.INTEGER;
        if (obj instanceof String)
            return (Serializer<K>) Serializer.STRING;

        return null;

    }

    private Serializer<V> findTypeVal(V obj) {
        if (obj instanceof Integer)
            return (Serializer<V>) Serializer.LONG;
        if (obj instanceof Integer)
            return (Serializer<V>) Serializer.INTEGER;
        if (obj instanceof String)
            return (Serializer<V>) Serializer.STRING;

        return null;

    }


    @Override
    public Set<Entry<K, V>> entrySet() {
        if (fastFileMapString != null) {
            ConcurrentMap<K, V> fastFileMapTT = (ConcurrentMap<K, V>) fastFileMapString;
            return fastFileMapTT.entrySet();
        }
        return hashMap.entrySet();
    }

    public Iterator getQueryTimeIterator() {
        if (queryTimeCache != null)
            return queryTimeCache.entrySet().iterator();
        return null;
    }

    public Set<Entry<K, String>> fastEntrySet() {
        return fastFileMapString.entrySet();
    }

    public Iterable cacheEntrySet() {
        return hashMap.entrySet();
    }


    @Override
    public int size() {
        if (!onlyMem)
            return hashMap.size() + fileHashMap.size();
        return hashMap.size();
    }

    public double getSizeGB() {
        return elemSize * size() / 1000000000;
    }


    public String getFileName() {
        return fileName;
    }

    public double getMemorySize() {
        if (hashMap == null)
            return 0;
        double s = ((hashMap.size() / 1000000) * elemSize) / 1000;
        return s;
    }

    public void open(K key, V val) {
        org.mapdb.Serializer<K> keyType = findTypeKey(key);
        org.mapdb.Serializer<V> valType = (Serializer<V>) Serializer.STRING;
        fastFileMapString = createFastFileMap(keyType, valType);
    }

    public void setExtraIndexType(IndexType extraIndexType) {
        this.extraIndexType = extraIndexType;
    }


    public void loadQueryTimeCahce() {
        queryTimeCache = new HashMap<K, V>();
        Iterator it = fastFileMapString.entrySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            K key = (K) pair.getKey();
            String value = (String) pair.getValue();
            ArrayList<Triple> tarr = deCompressedHyprid(value);
            queryTimeCache.put(key, (V) tarr);
            if (count % 100 == 0 && isMemoryLow()) {
                System.out.println("done, memory filled !");
                return;
            }
            if (count % 10000 == 0) {
                for (int i = 0; i < 50; i++) {
                    System.out.println();
                }
                System.out.println("loaded " + count / 1000 + " K keys.");
            }
            count++;
            //TODO remove
            if (count > 100000)
                break;
        }
        System.out.println("done, full load !");
        queryTimeCacheEnabled = true;
    }

    public static boolean isMemoryLow() {
        long rem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long max = Runtime.getRuntime().maxMemory();
        if ((max - rem) / 1000 < 2000000)
            return true;
        return false;
    }

    public boolean removeMatchingTriples(Triple triple, byte type) {
        int firstKey = getKeyIndex(type , true);
        if (firstKey >= 0) {
            remove(triple.triples[firstKey]);
            return true;
        }
        return false;
    }

    public void addTriples(Triple triple, byte type ,Dictionary dictionary) {
        int firstKey = getKeyIndex(type , true);
        if (firstKey >= 0) {
            if (containsKey(triple.triples[firstKey])) {
                ( (ArrayList<Triple>) ( get(triple.triples[firstKey]))).add(triple);
            } else {
                ArrayList<Triple> list = new ArrayList();
                list.add(triple);
                Integer codeObj = dictionary.get(dictionary.get(triple.triples[firstKey]));
                put((K)codeObj, (V)list);
            }
        }
    }

    private int getKeyIndex(byte type , boolean first) {
        int firstKey = -1, secondKey = -1;
        switch (type) {
            case IndexesPool.SPo:
                firstKey = 0;
                secondKey = 1;
                break;
            case IndexesPool.OPs:
                firstKey = 2;
                secondKey = 1;
                break;
            case IndexesPool.POs:
                firstKey = 1;
                secondKey = 2;
                break;
            case IndexesPool.SOp:
                firstKey = 0;
                secondKey = 2;
                break;
            case IndexesPool.OSp:
                firstKey = 2;
                secondKey = 0;
                break;
            case IndexesPool.PSo:
                firstKey = 1;
                secondKey = 0;
                break;
        }
        if(first)
            return firstKey;
        return secondKey;
    }

    public void putAndSort(K v, V list , int index1 , int index2) {
        sortArray((ArrayList<Triple>) list , index1 ,index2);
        put(v,list);
    }


    class Consumer extends Thread /*implements Runnable*/ {
        private BlockingQueue<Triple> sharedTripleQueue;
        private BlockingQueue<K> sharedkeyQueue;
        private BlockingQueue<String> sharedStrkeyQueue;
        private boolean stop = false;
        private K stopingVal;

        public Consumer() {
            this.sharedTripleQueue = new LinkedBlockingQueue<Triple>(maxCacheSize * 2);
            this.sharedkeyQueue = new LinkedBlockingQueue<K>(maxCacheSize * 2);


        }

        public int getBufferSize() {
            return sharedkeyQueue.size();
        }

        public void stopThread() {
            stop = true;
            if (stopingVal != null)
                sharedkeyQueue.add(stopingVal);
            sharedTripleQueue.add(new Triple(0, 0, 0));
        }

        public void addTriple(K key, Triple triple) {
            try {
                if (stopingVal == null)
                    stopingVal = key;
                sharedkeyQueue.put(key);
                sharedTripleQueue.put(triple);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void run() {
            while (!stop) {
                try {
                    Triple triple = sharedTripleQueue.take();
                    K key = sharedkeyQueue.take();
                    if (!stop)
                        addTripletoList(key, triple);
                    //System.out.println("Consumed: "+ num + ":by thread:"+threadNo);
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
        }
    }


    public static int binarySearch(ArrayList<Triple> arr, int index, int key, TriplePattern2.WithinIndex withinIndex) {
        int last = arr.size() - 1;
        int first = 0;
        int mid = (first + last) / 2;
        int cost = 0;
        while (first <= last) {
            cost++;
            if (arr.get(mid).triples[index] < key) {
                first = mid + 1;
            } else if (arr.get(mid).triples[index] == key) {
                // System.out.println("Element is found at index: " + mid);
                int found = arr.get(mid).triples[index];
                int t = mid;
                while (t > 0 && arr.get(t - 1).triples[index] == found) {
                    cost++;
                    t--;
                }
                withinIndex.index = t;
                return cost;
            } else {
                last = mid - 1;
            }
            mid = (first + last) / 2;
        }
        if (first > last) {
            return -1;
            // System.out.println("Element is not found!");
        }
        return -1;
    }

    private final static int keyLength = 4;


}

